from typing import Dict, Any, Optional, List, Callable, Union
import asyncio
import operator
import re
from lark import Lark, Transformer, v_args, Token
import mistql

from framework.service.flow import (
    asynchronous, synchronous, get_transaction_id, set_transaction_id, 
    _transaction_id, convert, get, put, format, route, normalize, framework_log, _load_resource
)
import framework.service.flow as flow
import framework.service.load as load

# --- 1. Grammar ---
grammar = r"""
    start: [braced_dict | top_level]
    top_level: (item ";")* -> dictionary
    braced_dict: "{" (item ";")* ";"? "}" -> dictionary
    POW_OP: "^"
    MUL_OP: "*" 
    DIV_OP: "/"
    MOD_OP: "%"
    ADD_OP: "+"
    SUB_OP: "-"
    COMPARISON_OP: "==" | "!=" | ">=" | "<=" | ">" | "<"
    PIPE: "|>"
    QUALIFIED_CNAME: CNAME ("." CNAME)+
    COMMENT: /#[^\n]*/
    ANY: "*"

    value: SIGNED_NUMBER -> number
        | (ESCAPED_STRING | SINGLE_QUOTED_STRING) -> string
        | ("true"i | "True"i) -> true | ("false"i | "False"i) -> false
        | ANY -> any_val
    
    ?power_expr: atom | power_expr POW_OP atom -> power
    ?mult_expr: power_expr | mult_expr (MUL_OP | DIV_OP | MOD_OP) power_expr -> binary_op
    ?add_expr: mult_expr | add_expr (ADD_OP | SUB_OP) mult_expr -> binary_op
    
    # Pipe level (lower than arithmetic, higher than comparison)
    ?pipe_expr: add_expr (PIPE (add_expr | tuple_inline))* -> pipe_node
    
    ?comparison_expr: pipe_expr | comparison_expr COMPARISON_OP pipe_expr -> binary_op
    ?not_expr: comparison_expr | "not" not_expr -> not_op
    ?and_expr: not_expr | and_expr ("and" | "&") not_expr -> and_op
    ?or_expr: and_expr | or_expr ("or" | "|") and_expr -> or_op
    
    ?expression: or_expr
    
    dictionary: braced_dict
    item: pair | function_call -> statement
    
    # Enforce typed name like integer:x for declarations
    typed_name: CNAME ":" (CNAME | QUALIFIED_CNAME) -> typed_name_node
    
    declaration: typed_name ":=" (expression | tuple_inline)
    mapping: (value | tuple_inline | typed_name | function_call | CNAME | QUALIFIED_CNAME) ":" (expression | tuple_inline)
    
    pair: "(" (declaration | mapping) ")" | (declaration | mapping)
    
    valid_tuple_item: value | dictionary | tuple | list | "(" expression ")" | typed_name | CNAME | QUALIFIED_CNAME
    tuple: "(" [ (expression | typed_name | CNAME | QUALIFIED_CNAME) ("," (expression | typed_name | CNAME | QUALIFIED_CNAME))* ","?] ")" -> tuple_
    list: "[" [ (expression | typed_name | CNAME | QUALIFIED_CNAME) ("," (expression | typed_name | CNAME | QUALIFIED_CNAME))* ","?] "]" -> list_
    function_call: (CNAME | QUALIFIED_CNAME | typed_name) "(" [call_args] ")"
    call_args: call_arg ("," call_arg)*
    call_arg: expression -> arg_pos | CNAME ":" expression -> arg_kw
    atom: value | dictionary | function_call | "(" (declaration | mapping) ")" -> pair | tuple | list | "(" expression ")" | typed_name | CNAME | QUALIFIED_CNAME -> simple_key
    tuple_inline: [valid_tuple_item ("," valid_tuple_item)* ","?] -> tuple_

    %import common.SIGNED_NUMBER
    %import common.ESCAPED_STRING
    %import common.CNAME
    %import common.WS
    SINGLE_QUOTED_STRING: /'[^']*'/
    %ignore WS
    %ignore COMMENT
"""

class DSLVariable:
    def __init__(self, name): self.name = name
    def __repr__(self): return f"VAR({self.name})"
    def __str__(self): return self.name

class ConfigTransformer(Transformer):
    def start(self, items): return items[0] if items else {}
    def call_args(self, args): return args
    def arg_pos(self, args): return ('POS', args[0])
    def arg_kw(self, args): return ('KW', str(args[0]), args[1])
    def pipe_node(self, items):
        p = [i for i in items if not (isinstance(i, Token) and i.type == 'PIPE')]
        return p[0] if len(p) == 1 else ('EXPRESSION', p)
    def declaration(self, args): return args[0], args[1]
    def mapping(self, args): return args[0], args[1]
    def function_call(self, args):
        name_node, call_args = args[0], (args[1] if len(args)>1 else [])
        if isinstance(name_node, tuple) and name_node[0] == 'TYPED':
            name = str(name_node[2])
        else:
            name = str(name_node)
        return ('CALL', name, tuple(a[1] for a in call_args if a[0]=='POS'), {a[1]: a[2] for a in call_args if a[0]=='KW'})
    def pair(self, args):
        return args[0]
    def statement(self, args):
        return args[0]
    def atom(self, args):
        # Strip parentheses if present (children would be [Token('('), item, Token(')')])
        # Or just return the first non-token item
        for a in args:
            if not isinstance(a, Token) or a.type not in ('LPAR', 'RPAR', 'LSQB', 'RSQB', 'LBRACE', 'RBRACE', 'COLON', 'COMMA', 'SEMICOLON'):
                return a
        return args[0]
    def dictionary(self, items):
        # Prevent recursive parsing of already transformed dictionaries
        if len(items) == 1 and isinstance(items[0], dict):
            return items[0]
            
        res = {}
        triggers = []
        for i in items:
            if isinstance(i, tuple) and len(i) == 2:
                k, v = i
                # Check for triggers: k is a function call or a tuple containing '*'
                is_event_trigger = isinstance(k, tuple) and len(k) > 0 and k[0] == 'CALL'
                is_cron_trigger = isinstance(k, tuple) and any(x == '*' for x in k if isinstance(x, str))
                
                def extract_typed(x):
                    if isinstance(x, tuple):
                        if len(x) >= 3 and x[0] == 'TYPED': return x
                        if len(x) == 1: return extract_typed(x[0])
                    return None

                t_node = extract_typed(k)
                
                # Normalize key to string if it's a Token or simple wrapper
                def normalize_key(x):
                    if hasattr(x, 'type'): return str(x)
                    if isinstance(x, DSLVariable): return str(x.name)
                    if isinstance(x, (list, tuple)) and len(x) == 1: return normalize_key(x[0])
                    return str(x)

                if is_event_trigger or is_cron_trigger:
                    triggers.append((k, v))
                elif t_node:
                    res[t_node] = v
                else:
                    res[normalize_key(k)] = v
            elif isinstance(i, tuple) and i[0] == 'CALL':
                # Use a random key or the function name as key for statements
                res[f"__stmt_{i[1]}"] = i
            elif isinstance(i, dict):
                # Another fail-safe for already parsed dictionaries in the list
                res.update(i)
            else:
                # Fallback
                pass
        
        if triggers:
            res['__triggers__'] = triggers
        return res
    def binary_op(self, args):
        l, o, r = args[0], args[1], args[2]
        m = {'+':'ADD','-':'SUB','*':'MUL','/':'DIV','%':'MOD','==':'EQ','!=':'NEQ','>=':'GTE','<=':'LTE','>':'GT','<':'LT'}
        return (f'OP_{m[str(o)]}', l, r)
    def power(self, args): return ('OP_POW', args[0], args[2])
    def and_op(self, args): return ('OP_AND', args[0], args[2] if len(args) > 2 else args[1])
    def or_op(self, args): return ('OP_OR', args[0], args[2] if len(args) > 2 else args[1])
    def not_op(self, args): return ('OP_NOT', args[1] if len(args) > 1 else args[0])
    def pipe_node(self, items):
        p = [i for i in items if not (isinstance(i, Token) and i.type == 'PIPE')]
        return p[0] if len(p) == 1 else ('EXPRESSION', p)
    def tuple_(self, items):
        items = [i for i in items if i is not None]
        if len(items) == 1: return items[0]
        return tuple(items)
    def list_(self, items):
        items = [i for i in items if i is not None]
        return list(items)
    def number(self, n): return float(str(n[0])) if '.' in str(n[0]) else int(str(n[0]))
    def string(self, s): return str(s[0]).strip('"\'')
    def true(self, _): return True
    def false(self, _): return False
    def simple_key(self, s): 
        # For qualified names or simple keys, we treat them as variables
        # Ensure we take the string value if it's a list or token
        val = str(s[0] if isinstance(s, list) else s)
        return DSLVariable(val)
    def any_val(self, _): return '*'
    def typed_name_node(self, args): return ('TYPED', str(args[0]), str(args[1]))
    def pair(self, args): return args[0]
    def __default__(self, data, children, meta):
        return children[0] if len(children) == 1 else children

class DSLVisitor:
    def __init__(self, functions_map=None):
        self.functions_map = functions_map or {}
        self.root_data = {}
        self.ops = {
            'OP_ADD': operator.add, 'OP_SUB': operator.sub, 'OP_MUL': operator.mul,
            'OP_DIV': operator.truediv, 'OP_MOD': operator.mod, 'OP_POW': operator.pow,
            'OP_EQ': operator.eq, 'OP_NEQ': operator.ne, 'OP_GT': operator.gt,
            'OP_LT': operator.lt, 'OP_GTE': operator.ge, 'OP_LTE': operator.le,
            'OP_AND': lambda a, b: a and b, 'OP_OR': lambda a, b: a or b, 'OP_NOT': lambda a: not a
        }
        self.root_path = "."
        self._background_tasks = []

    async def run(self, data):
        self.root_data = data
        # Inject DSL executor for flow support at root level
        self.root_data['__execute_dsl_function__'] = self.execute_dsl_function
        res = await self.visit(data)
        
        # Se abbiamo dei task in background (cron, event), attendiamo che finiscano
        # Questo mantiene vivo il processo se ci sono listener attivi.
        if self._background_tasks:
            framework_log("INFO", f"⏳ In attesa di {len(self._background_tasks)} task in background...", emoji="💤")
            await asyncio.gather(*self._background_tasks)
            
        return res

    @staticmethod
    def wildcard_match(data, pattern):
        """
        Matches data against a pattern with wildcards (*, ?).
        Supports:
        - String patterns: "user.*" matches "user.login", "user.logout"
        - List patterns: ["*", "*", "15", "*", "*"] for cron-like matches
        """
        if isinstance(pattern, str):
            # Convert glob pattern to regex
            regex_pattern = re.escape(pattern).replace(r'\*', '.*').replace(r'\?', '.')
            return bool(re.fullmatch(regex_pattern, str(data)))
        
        if isinstance(pattern, (list, tuple)) and isinstance(data, (list, tuple)):
            if len(pattern) != len(data):
                return False
            for p, d in zip(pattern, data):
                if p == '*':
                    continue
                if str(p) != str(d):
                    return False
            return True
            
        return str(data) == str(pattern)

    async def _resolve(self, node, ctx):
        # Treat Token and DSLVariable similarly
        if not hasattr(node, 'type') and type(node).__name__ != 'DSLVariable' and not isinstance(node, str):
            return node
            
        name = str(node.name if hasattr(node, 'name') else node)
        
        # Handle dot notation (e.g., service.config.timeout)
        if '.' in name:
            parts = name.split('.')
            val = await self._resolve(parts[0], ctx)
            for part in parts[1:]:
                if isinstance(val, dict):
                    val = val.get(part)
                else:
                    val = getattr(val, part, None)
                if val is None: break
            return val

        if ctx and name in ctx: return await self.visit(ctx[name], ctx)
        
        # Priority to functions map
        if name in self.functions_map:
            return self.functions_map[name]
            
        # Check in root_data (handling typed names)
        if name in self.root_data: 
            return await self.visit(self.root_data[name], ctx)
        
        for k in self.root_data:
            if isinstance(k, tuple) and len(k) == 3 and k[0] == 'TYPED' and k[2] == name:
                return await self.visit(self.root_data[k], ctx)

        # Fallback to standard types if not shadowed
        type_map = {'dict': dict, 'list': list, 'str': str, 'int': int, 'float': float, 'bool': bool, 'any': object, 'tuple': tuple}
        if name in type_map:
            return type_map[name]
        
        return name

    def _validate_type(self, value, type_name, var_name):
        """Validates that a value matches the declared type name."""
        type_map = {
            'int': int, 'integer': int, 'i8': int, 'i16': int, 'i32': int, 'i64': int, 'i128': int,
            'str': str, 'string': str,
            'dict': dict, 'list': list, 'float': float,'f8': float, 'f16': float, 'f32': float, 'f64': float, 'f128': float,
            'tuple': tuple, 'array': list,
            'bool': bool, 'boolean': bool, 
            'any': object, 'number': (int, float)
        }
        
        expected_type = type_map.get(type_name)
        if expected_type is None:
            return
            
        if expected_type is object:
            return
            
        # Forgiving numeric validation: auto-convert float to int if whole number
        if expected_type is int and isinstance(value, float) and value.is_integer():
            # In-place "fix" might be better handled during visit, but for validation we can allow it
            # Actually, let's just check if it matches the expected type or the forgiven case
            return

        if not isinstance(value, expected_type):
            raise TypeError(f"Errore di tipo: la variabile '{var_name}' è dichiarata come {type_name}, ma ha valore {type(value).__name__} ('{value}')")

    async def visit(self, node, ctx=None):
        if isinstance(node, dict):
            # Use a working context to allow references to previous definitions in the same block
            working_ctx = (ctx or {}).copy()
            # Inject DSL executor for flow support
            working_ctx['__execute_dsl_function__'] = self.execute_dsl_function
            res = {}
            
            # Extract triggers first but evaluate them later
            triggers = node.pop('__triggers__', [])
            
            # 1. Resolve all items sequentially, updating the working context
            for k, v in node.items():
                val = await self.visit(v, working_ctx)
                if isinstance(k, tuple) and len(k) == 3 and k[0] == 'TYPED':
                    _, type_name, name = k
                    self._validate_type(val, type_name, name)
                    res[name] = val
                    working_ctx[name] = val
                    # print(f"DEBUG: Assigned {name} (typed {type_name}) = {val}")
                else:
                    name_str = str(k)
                    res[name_str] = val
                    working_ctx[name_str] = val
                    # print(f"DEBUG: Assigned {name_str} = {val}")
            
            # 2. Start triggers (concurrent)
            for trigger_key, action in triggers:
                task = asyncio.create_task(self._start_trigger(trigger_key, action, working_ctx))
                self._background_tasks.append(task)
            
            return res
        if isinstance(node, list): return [await self.visit(x, ctx) for x in node]
        if isinstance(node, tuple) and node:
            tag = node[0]
            if isinstance(tag, str) and tag in self.ops:
                return self.ops[tag](*[await self.visit(a, ctx) for a in node[1:]])
            if tag == 'EXPRESSION': return await self.evaluate_expression(node[1], ctx)
            if tag == 'CALL': return await self.execute_call(node, ctx)
            if tag == 'TYPED':
                # Resolving a typed name reference
                _, type_name, name = node
                val = await self._resolve(name, ctx)
                # Validation could be added here if needed
                return val
            
            # Detect function definition: (args), {body}, (returns)
            # We must NOT visit the body (dict) now, and we shouldn't resolve the signature yet
            if len(node) == 3 and isinstance(node[1], dict): 
                return node
                
            return tuple([await self.visit(x, ctx) for x in node])
        
        # If it's a Token or string identifier, treat it as a variable name to resolve
        if hasattr(node, 'type') or type(node).__name__ == 'DSLVariable': 
            return await self._resolve(node, ctx)
            
        return node

    async def _start_trigger(self, trigger_key, action, ctx):
        """Starts a background loop for a cron or event trigger."""
        if isinstance(trigger_key, tuple) and trigger_key[0] == 'CALL':
            # Event trigger
            await self._event_loop(trigger_key, action, ctx)
        elif isinstance(trigger_key, (list, tuple)) and any(x == '*' for x in trigger_key):
            # Cron trigger
            await self._cron_loop(trigger_key, action, ctx)

    async def _cron_loop(self, pattern, action, ctx):
        """Loop for cron tasks."""
        framework_log("INFO", f"⏰ Avvio task cron: {pattern}", emoji="⏳")
        while True:
            # Sleep until next minute starts to be more precise or just periodic check
            import datetime
            now = datetime.datetime.now()
            # pattern: (min, hour, day, month, weekday)
            current = (now.minute, now.hour, now.day, now.month, now.weekday())
            
            if self.wildcard_match(current, pattern):
                framework_log("INFO", f"⚡ Esecuzione task cron: {pattern}", emoji="⚡")
                try:
                    await self.visit(action, ctx)
                except Exception as e:
                    framework_log("ERROR", f"❌ Errore task cron {pattern}: {e}", emoji="❌")
            
            # Sleep until next minute starts (Wait for next minute)
            import datetime
            now = datetime.datetime.now()
            wait_seconds = 60 - now.second
            if wait_seconds <= 0: wait_seconds = 60
            await asyncio.sleep(wait_seconds)

    async def _event_loop(self, call_node, action, ctx):
        """Loop for event tasks."""
        _, name, p_nodes, k_nodes = call_node
        framework_log("INFO", f"🎭 Avvio listener evento: {name}", emoji="👂")
        
        while True:
            try:
                # Esegue la chiamata (es: messenger.read) e usa il risultato come trigger
                # Se è un polling o un'attesa, la funzione stessa gestirà il tempo
                res = await self.execute_call(call_node, ctx)
                
                # Check for success and presence of non-empty data
                is_valid = res and isinstance(res, dict) and res.get('success')
                data = res.get('data') if is_valid else None
                
                if is_valid and data:
                    framework_log("INFO", f"🔔 Evento rilevato: {name}", emoji="🔔")
                    new_ctx = (ctx or {}).copy()
                    new_ctx['@event'] = data
                    await self.visit(action, new_ctx)
                else:
                    await asyncio.sleep(1)
            except Exception as e:
                framework_log("ERROR", f"❌ Errore listener evento {name}: {e}", emoji="❌")
                await asyncio.sleep(5)

    async def execute_call(self, call, ctx):
        _, name_node, p_nodes, k_nodes = call
        
        # Resolve the function definition first
        func_def = await self._resolve(name_node, ctx)
        
        p_args = [await self.visit(a, ctx) for a in p_nodes]
        k_args = {k: await self.visit(v, ctx) for k, v in k_nodes.items()}
        
        if isinstance(func_def, (list, tuple)) and len(func_def) == 3 and isinstance(func_def[1], dict):
            return await self.execute_dsl_function(func_def, p_args, k_args)
            
        # If it's a string, use standard execution
        name = str(name_node.name if hasattr(name_node, 'name') else name_node)
        return await self._execute(name, p_args, k_args, ctx=ctx)

    async def _execute(self, name, p_args, k_args, ctx=None):
        name = str(name)
        # Resolve any remaining DSLVariables in arguments
        p_args = [(await self.visit(a)) if type(a).__name__ == 'DSLVariable' else a for a in p_args]
        k_args = {k: (await self.visit(v)) if type(v).__name__ == 'DSLVariable' else v for k, v in k_args.items()}
        
        func = self.functions_map.get(name)
        if name == 'include' and p_args:
            path = p_args[0]
            if not path.endswith(".dsl"): path += ".dsl"
            try:
                content = await _load_resource(path=path)
                from lark import Lark
                new_data = Lark(grammar).parse(content)
                new_dict = ConfigTransformer().transform(new_data)
                # Merge into root_data without modifying during iteration
                for k, v in new_dict.items():
                    if not k.startswith('__stmt_'):
                        self.root_data[k] = v
                return {"included": path, "variables": list(new_dict.keys())}
            except Exception as e:
                framework_log("ERROR", f"Failed to include {path}: {e}", emoji="❌")
                return {"error": str(e)}

        if name in ('foreach', 'map') and len(p_args) >= 4:
            # Recompose split macro definition
            data = p_args[0]
            func_triple = (p_args[1], p_args[2], p_args[3])
            if name == 'foreach': return await self._dsl_foreach(data, func_triple, ctx=ctx)
            return await self._dsl_map(data, func_triple, ctx=ctx)

        if name == 'foreach' and p_args:
            return await self._dsl_foreach(p_args[0], p_args[1] if len(p_args)>1 else None, ctx=ctx)
        
        if name == 'map' and p_args:
            return await self._dsl_map(p_args[0], p_args[1] if len(p_args)>1 else None, ctx=ctx)

        # Handle qualified names (e.g., executor.all_completed)
        if '.' in name:
            parts = name.split('.')
            obj = self.functions_map.get(parts[0])
            
            if obj is None:
                # Try context or root data
                obj = (ctx or {}).get(parts[0]) or self.root_data.get(parts[0])
                if obj is None:
                    # Try typed name in root
                    for k in self.root_data:
                        if isinstance(k, tuple) and len(k) == 3 and k[0] == 'TYPED' and k[2] == parts[0]:
                            obj = self.root_data[k]
                            break

            if obj:
                # Navigate through the parts to get the final method
                for part in parts[1:]:
                    if isinstance(obj, dict):
                        obj = obj.get(part)
                    else:
                        obj = getattr(obj, part, None)
                    
                    if obj is None:
                        framework_log("ERROR", f"Attribute {part} not found on {parts[0]}", emoji="🤷")
                        return None
                        
                # Now obj is the method we want to call
                if isinstance(obj, tuple) and len(obj) == 3 and isinstance(obj[1], dict):
                     return await self.execute_dsl_function(obj, p_args, k_args)

                if callable(obj):
                    res = obj(*p_args, **k_args)
                    return await res if asyncio.iscoroutine(res) else res
                return obj

        if func:
            res = func(*p_args, **k_args)
            result = await res if asyncio.iscoroutine(res) else res
            # Auto-unwrap transactional results (success or ok)
            if isinstance(result, dict):
                is_success = result.get('success') or result.get('ok')
                if is_success is True and 'data' in result:
                    return result['data']
            return result
            
        # Resolve from context or root
        dsl_func = (ctx or {}).get(name) or self.root_data.get(name)
        if dsl_func is None:
            # Try typed name lookup in root
            for k in self.root_data:
                if isinstance(k, tuple) and len(k) == 3 and k[0] == 'TYPED' and k[2] == name:
                    dsl_func = self.root_data[k]
                    break
                    
        if isinstance(dsl_func, tuple) and len(dsl_func) == 3:
            return await self.execute_dsl_function(dsl_func, p_args, k_args)
        
        if callable(dsl_func):
            res = dsl_func(*p_args, **k_args)
            return await res if asyncio.iscoroutine(res) else res
            
        framework_log("ERROR", f"Function {name} not found", emoji="🤷")
        return None

    async def _dsl_foreach(self, data, func, ctx=None):
        if not isinstance(data, (list, tuple, dict)): return []
        items = list(data.values()) if isinstance(data, dict) else list(data)
        
        # Se func è una funzione DSL (tupla di 3 con dict centrale)
        if isinstance(func, (list, tuple)) and len(func) == 3 and isinstance(func[1], dict):
            results = []
            for item in items:
                res = await self.execute_dsl_function(func, [item])
                results.append(res)
            return results
        
        # Altrimenti usa flow.foreach standard
        return await flow.foreach(data, func, context=ctx)

    async def _dsl_map(self, data, func, ctx=None):
        if not isinstance(data, (list, tuple, dict)): return data
        
        # Se func è una funzione DSL
        if isinstance(func, (list, tuple)) and len(func) == 3 and isinstance(func[1], dict):
            return [await self.execute_dsl_function(func, [i]) for i in data]
        
        # Se è una stringa, usa MistQL
        if isinstance(func, str):
            # Resolve data for MistQL
            return [mistql.query(func, data=i) for i in (data if isinstance(data, list) else [data])]
            
        return data

    async def evaluate_expression(self, ops, ctx):
        if not ops: return None
        # Always merge with root_data for core features like DSL executor
        context = self.root_data.copy()
        if ctx: context.update(ctx)
        
        seed = await self.visit(ops[0], context)
        stages = [] # We'll build stages carefully
        
        # Initial stage just returns the seed
        stages.append(flow.step(lambda context=None: seed))

        for op in ops[1:]:
            # Use a closure to capture the current op
            def make_stage(_op):
                async def pipe_stage(context=None):
                    # Get previous stage result correctly from context
                    prev_raw = context['outputs'][-1] if context and context.get('outputs') else seed
                    
                    # Auto-unwrapping for transactional outputs
                    prev = (prev_raw.get('data') 
                            if isinstance(prev_raw, dict) and (prev_raw.get('success') is True or prev_raw.get('ok') is True) and 'data' in prev_raw 
                            else prev_raw)
                    
                    name = None
                    p_nodes = []
                    k_nodes = {}
                    
                    if isinstance(_op, tuple) and _op[0] == 'CALL':
                        _, name, p_nodes, k_nodes = _op
                    elif isinstance(_op, tuple) and _op[0] == 'TYPED':
                        name = str(_op[2])
                    elif isinstance(_op, DSLVariable):
                        name = str(_op.name)
                    elif isinstance(_op, str):
                        name = _op
                    elif isinstance(_op, (list, tuple)) and len(_op) == 3 and isinstance(_op[1], dict):
                        # Anonymous function def in pipe
                        return await self.execute_dsl_function(_op, [prev], {})
                    
                    if name and isinstance(name, str):
                        # Handle split DSL functions in p_nodes
                        if len(p_nodes) == 3 and isinstance(p_nodes[1], dict):
                             p_args = [prev, p_nodes]
                        elif len(p_nodes) == 1 and hasattr(p_nodes[0], 'data') and p_nodes[0].data == 'dictionary':
                             p_args = [prev, p_nodes[0]]
                        else:
                             p_args = [prev] + [await self.visit(a, context) for a in p_nodes]
                             
                        k_args = {k: await self.visit(v, context) for k, v in k_nodes.items()}
                        
                        # Resolve function definition from context
                        func_def = await self._resolve(name, context)
                        
                        if isinstance(func_def, (list, tuple)) and len(func_def) == 3 and isinstance(func_def[1], dict):
                            return await self.execute_dsl_function(func_def, p_args, k_args)
                            
                        # Use _execute for library calls or single names
                        res = await self._execute(name, p_args, k_args, ctx=context)
                        # Auto-unwrap pipe transit
                        if isinstance(res, dict) and (res.get('success') is True or res.get('ok') is True) and 'data' in res:
                            res = res['data']
                        return res
                    
                    # Fallback for simple values/variables in pipe
                    return await self.visit(_op, context)
                
                pipe_stage.__name__ = f"dsl_stage_{str(_op)[:20]}"
                return pipe_stage
            
            stages.append(flow.step(make_stage(op)))

        try:
            return await flow.pipe(*stages, context=context)
        except Exception as e:
            framework_log("ERROR", f"Exception during DSL expression evaluation: {e}", emoji="💥")
            import traceback
            traceback.print_exc()
            return {"success": False, "errors": [str(e)]}

    async def execute_dsl_function(self, func_def, p_args, k_args=None):
        in_def, body, out_def = func_def
        ctx = {}
        p_args = p_args or []
        k_args = k_args or {}
        
        def get_p(p):
            if isinstance(p, tuple) and p[0] == 'TYPED': return (p[2], p[1])
            return (p.name if isinstance(p, DSLVariable) else str(p), None)
            
        def is_multi(d):
            if not isinstance(d, (list, tuple)) or not d: return False
            if len(d) == 3 and d[0] == 'TYPED': return False
            return True

        params = [get_p(p) for p in in_def] if is_multi(in_def) else [get_p(in_def)]
        
        # 1. Map positional arguments
        for i, (name, type_name) in enumerate(params):
            val = None
            if i < len(p_args):
                val = p_args[i]
            elif name in k_args:
                val = k_args.pop(name)
            
            if type_name:
                t_map = {'int':int,'integer':int,'str':str,'string':str,'float':float,'number':(int,float),'bool':bool,'boolean':bool,'dict':dict,'list':list,'tuple':tuple}
                exp = t_map.get(type_name) or self.functions_map.get(type_name)
                if exp and not isinstance(val, exp): 
                    raise TypeError(f"Parametro {name} atteso {type_name}, ricevuto {type(val).__name__}")
            ctx[name] = val
            
        # 2. Execute body using the visitor's dictionary logic
        body_res = await self.visit(body, ctx)
        ctx.update(body_res)
        
        # 3. Return outputs
        outs = [get_p(p)[0] for p in out_def] if is_multi(out_def) else [get_p(out_def)[0]]
        res = []
        for o in outs:
            val = ctx.get(o)
            print(f"DEBUG: Function output lookup for '{o}': {val} (found in ctx: {o in ctx})")
            # If not found directly, try to resolve as a variable just in case
            if val is None:
                val = await self._resolve(o, ctx)
                print(f"DEBUG: Resolved output '{o}' to: {val}")
            res.append(val)
        
        return res[0] if len(res) == 1 else tuple(res)

class LazyService:
    """Proxy that lazily loads a service from the container and allows dot-notation calls."""
    def __init__(self, service_name):
        self._service_name = service_name
        self._instance = None

    async def _get_instance(self):
        if self._instance is None:
            import framework.service.context as context
            # Poll until the service is registered in the container (with timeout)
            attempts = 0
            while not hasattr(context.container, self._service_name) and attempts < 20:
                await asyncio.sleep(0.5)
                attempts += 1
            
            if not hasattr(context.container, self._service_name):
                framework_log("WARNING", f"⚠️ Servizio '{self._service_name}' non trovato nel container dopo 10 secondi.", emoji="⏳")
                return None
            # Call the provider to get the instance
            self._instance = getattr(context.container, self._service_name)()
        return self._instance

    def __getattr__(self, name):
        # Return a dispatcher that waits for the instance
        async def dispatcher(*args, **kwargs):
            instance = await self._get_instance()
            if instance is None:
                framework_log("ERROR", f"❌ Impossibile chiamare '{name}' su servizio '{self._service_name}': istanza non trovata.")
                return {"success": False, "errors": ["Service not found"]}
            attr = getattr(instance, name)
            if callable(attr):
                res = attr(*args, **kwargs)
                return await res if asyncio.iscoroutine(res) else res
            return attr
        return dispatcher

    async def __call__(self, *args, **kwargs):
        # Direct call returns the instance (waiting if necessary)
        return await self._get_instance()

dsl_functions = {
    'resource': load.resource,
    'transform': flow.transform,
    'normalize': flow.normalize,
    'put': flow.put,
    'format': flow.format, 'foreach': flow.foreach, 'convert': flow.convert, 'get': flow.get,
    'keys': lambda d: list(d.keys()) if isinstance(d, dict) else [],
    'values': lambda d: list(d.values()) if isinstance(d, dict) else [],
    'items': lambda d: list(d.items()) if isinstance(d, dict) else [],
    'print': lambda d: (print(f"*** CUSTOM PRINT ***: {d}"), d)[1],
    'pick': lambda d, keys: {k: v for k, v in d.items() if k in keys} if isinstance(d, dict) and isinstance(keys, (list, tuple)) else d,
    'filter': lambda d, keys: {k: v for k, v in d.items() if k in keys} if isinstance(d, dict) and isinstance(keys, (list, tuple)) else d,
    'match': flow._dsl_switch,
    'batch': flow.batch, 'parallel': flow.batch,
    'race': flow.race, 'timeout': flow.timeout, 'throttle': flow.throttle,
    'catch': flow.catch, 'branch': flow.branch, 'retry': flow.retry,
    'fallback': flow.fallback,
    'remap': lambda data, *names: [dict(zip(names, item)) for item in data] if isinstance(data, (list, tuple)) else data,
    'entries': lambda d: list(d.items()) if isinstance(d, dict) else [],
    'merge': lambda a, b: (
        (a | b) if isinstance(a, dict) and isinstance(b, dict) else 
        ((list(a) if isinstance(a, (list, tuple)) else [a]) + (list(b) if isinstance(b, (list, tuple)) else [b]))
    ),
    'concat': lambda a, b: ((list(a) if isinstance(a, (list, tuple)) else [a]) + (list(b) if isinstance(b, (list, tuple)) else [b])),
    'query': lambda data, q: mistql.query(q, data=data),
    'messenger': LazyService('messenger'),
    'executor': LazyService('executor'),
    **{k: v for k, v in zip(['dict','list','str','int','float','bool'], [dict,list,str,int,float,bool])},
    'not': lambda x: not x,
    'integer':int,'string':str,'boolean':bool,'number':float,'relative':int,'natural':int,'rational':float,'complex':float
}

def parse_dsl_file(content):
    return ConfigTransformer().transform(Lark(grammar, parser='earley').parse(content))

async def execute_dsl_file(content_or_parsed):
    parsed = parse_dsl_file(content_or_parsed) if isinstance(content_or_parsed, str) else content_or_parsed
    return await DSLVisitor(dsl_functions).run(parsed)

async def run_dsl_tests(visitor, parsed_data):
    test_suite = parsed_data.get('test_suite', [])
    if isinstance(test_suite, dict): test_suite = [test_suite]
    if not isinstance(test_suite, (list, tuple)): return False
    all_passed = True
    print("\n" + "="*40 + f"\nDSL Tests: {len(test_suite)}\n" + "="*40)
    for test in test_suite:
        if not isinstance(test, dict): continue
        target, args, expected = test.get('target'), (test.get('input') if test.get('input') is not None else test.get('input_args')), (test.get('output') if test.get('output') is not None else test.get('expected_output'))
        print(f"Testing '{target}'...", end=" ")
        try:
            target_def = parsed_data.get(target)
            '''if target_def is None:
                target_def = target
                print(args)
                ok = await visitor.execute_dsl_function(target_def, *args)
                print("----",ok)
            print(test,"<----")
            print(target_def)'''
            actual = await visitor.execute_dsl_function(target_def, args) if isinstance(target_def, tuple) and len(target_def) == 3 and isinstance(target_def[1], dict) else await visitor.visit(target_def)
            
            if actual == expected: print("🟢 OK")
            else: print(f"🔴 FAILED (expected {expected}, got {actual})"); all_passed = False
        except Exception as e:
            # import traceback
            # traceback.print_exc()
            print(f"🔴 EXC: {e}"); all_passed = False
    print("="*40 + f"\nRESULT: {'🟢 PASSED' if all_passed else '🔴 FAILED'}\n" + "="*40)
    return all_passed
