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

# --- 1. Grammar ---
grammar = r"""
    start: [dictionary]
    POW_OP: "^"
    MUL_OP: "*" 
    DIV_OP: "/"
    MOD_OP: "%"
    ADD_OP: "+"
    SUB_OP: "-"
    COMPARISON_OP: "==" | "!=" | ">=" | "<=" | ">" | "<"
    PIPE: "|"
    QUALIFIED_CNAME: CNAME ("." CNAME)+
    COMMENT: /#[^\n]*/

    value: SIGNED_NUMBER -> number
        | ESCAPED_STRING -> string
        | "Vero" -> true | "Falso" -> false
        | (CNAME | QUALIFIED_CNAME) -> simple_key
    
    not_expr: atom | "not" not_expr -> not_op
    power_expr: not_expr | power_expr POW_OP not_expr -> power
    mult_expr: power_expr | mult_expr (MUL_OP | DIV_OP | MOD_OP) power_expr -> binary_op
    add_expr: mult_expr | add_expr (ADD_OP | SUB_OP) mult_expr -> binary_op
    comparison_expr: add_expr | comparison_expr COMPARISON_OP add_expr -> binary_op
    and_expr: comparison_expr | and_expr "and" comparison_expr -> and_op
    or_expr: and_expr | or_expr "or" and_expr -> or_op
    
    dictionary.10: "{" (item ";")* ";"? "}" | (item ";")*
    item: pair | function_call -> statement
    
    # Typed name like integer:x
    typed_name: CNAME ":" CNAME -> typed_name_node
    
    pair_statement.10: (value | tuple_inline | typed_name) ":" (expression | tuple_inline | typed_name)
    pair: "(" pair_statement ")" | pair_statement
    valid_tuple_item: value | dictionary | tuple | "(" or_expr ")" | typed_name
    tuple: "(" [ (expression | typed_name) ("," (expression | typed_name))*] ")" -> tuple_
    function_call: (CNAME | QUALIFIED_CNAME) "(" [call_args] ")"
    call_args: call_arg ("," call_arg)*
    call_arg: expression -> arg_pos | CNAME ":" expression -> arg_kw
    atom: value | dictionary | function_call | "(" pair_statement ")" -> pair | tuple | "(" or_expr ")" | typed_name
    tuple_inline: [valid_tuple_item "," valid_tuple_item ("," valid_tuple_item)*] -> tuple_
    expression: or_expr (PIPE (or_expr | tuple_inline))* -> expression

    %import common.SIGNED_NUMBER
    %import common.ESCAPED_STRING
    %import common.CNAME
    %import common.WS
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
    def function_call(self, args):
        name, call_args = args[0], (args[1] if len(args)>1 else [])
        return ('CALL', str(name), tuple(a[1] for a in call_args if a[0]=='POS'), {a[1]: a[2] for a in call_args if a[0]=='KW'})
    def pair_statement(self, args):
        k, v = args
        return (str(k.name) if isinstance(k, DSLVariable) else str(k)), v
    def statement(self, args):
        return args[0]
    def dictionary(self, items):
        res = {}
        for i in items:
            if isinstance(i, tuple) and len(i) == 2 and not isinstance(i[0], str) and i[0][0] == 'CALL':
                # It's a statement (function call)
                pass # We handle it later in evaluation or we can put it in a special key
            if isinstance(i, tuple) and len(i) == 2 and isinstance(i[0], str):
                res[i[0]] = i[1]
            elif isinstance(i, tuple) and i[0] == 'CALL':
                # Use a random key or the function name as key for statements
                res[f"__stmt_{i[1]}"] = i
            else:
                # Fallback
                pass
        return res
    def binary_op(self, args):
        l, o, r = args
        m = {'+':'ADD','-':'SUB','*':'MUL','/':'DIV','%':'MOD','==':'EQ','!=':'NEQ','>=':'GTE','<=':'LTE','>':'GT','<':'LT'}
        return (f'OP_{m[str(o)]}', l, r)
    def power(self, args): return ('OP_POW', args[0], args[2])
    def and_op(self, args): return ('OP_AND', args[0], args[1])
    def or_op(self, args): return ('OP_OR', args[0], args[1])
    def not_op(self, args): return ('OP_NOT', args[0])
    def expression(self, items):
        p = [i for i in items if not (isinstance(i, Token) and i.type == 'PIPE')]
        return p[0] if len(p) == 1 else ('EXPRESSION', p)
    def tuple_(self, items):
        items = [i for i in items if i is not None]
        return items[0] if len(items) == 1 else tuple(items)
    def number(self, n): return float(str(n[0])) if '.' in str(n[0]) else int(str(n[0]))
    def string(self, s): return str(s[0]).strip('"')
    def true(self, _): return True
    def false(self, _): return False
    def simple_key(self, s): return DSLVariable(str(s[0]))
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

    async def run(self, data):
        self.root_data = data
        return await self.visit(data)

    async def _resolve(self, node, ctx):
        if not isinstance(node, DSLVariable): return node
        if ctx and node.name in ctx: return await self.visit(ctx[node.name], ctx)
        if node.name in self.root_data: return await self.visit(self.root_data[node.name], ctx)
        return self.functions_map.get(node.name, node.name)

    async def visit(self, node, ctx=None):
        if isinstance(node, dict):
            # Create a copy of items to avoid "dictionary changed size during iteration"
            items = list(node.items())
            return {k: await self.visit(v, ctx) for k, v in items}
        if isinstance(node, list): return [await self.visit(x, ctx) for x in node]
        if isinstance(node, tuple) and node:
            tag = node[0]
            if isinstance(tag, str) and tag in self.ops:
                return self.ops[tag](*[await self.visit(a, ctx) for a in node[1:]])
            if tag == 'EXPRESSION': return await self.evaluate_expression(node[1], ctx)
            if tag == 'CALL': return await self.execute_call(node, ctx)
            if tag == 'TYPED': return node # Keep typed node for execute_dsl_function
            if len(node) == 3 and isinstance(node[1], dict): 
                return (await self.visit(node[0], ctx), node[1], await self.visit(node[2], ctx))
            return tuple([await self.visit(x, ctx) for x in node])
        return await self._resolve(node, ctx) if isinstance(node, DSLVariable) else node

    async def execute_call(self, call, ctx):
        _, name, p_nodes, k_nodes = call
        p_args = [await self.visit(a, ctx) for a in p_nodes]
        k_args = {k: await self.visit(v, ctx) for k, v in k_nodes.items()}
        return await self._execute(name, p_args, k_args)

    async def _execute(self, name, p_args, k_args):
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


        # Handle qualified names (e.g., executor.all_completed)
        if '.' in name:
            parts = name.split('.')
            obj = self.functions_map.get(parts[0])
            if obj:
                # Navigate through the parts to get the final method
                for part in parts[1:]:
                    if hasattr(obj, part):
                        obj = getattr(obj, part)
                    else:
                        framework_log("ERROR", f"Attribute {part} not found on {parts[0]}", emoji="🤷")
                        return None
                # Now obj is the method we want to call
                if callable(obj):
                    res = obj(*p_args, **k_args)
                    return await res if asyncio.iscoroutine(res) else res
                return obj

        if func:
            res = func(*p_args, **k_args)
            return await res if asyncio.iscoroutine(res) else res
        dsl_func = self.root_data.get(name)
        if isinstance(dsl_func, tuple) and len(dsl_func) == 3:
            inp = p_args[0] if len(p_args) == 1 and not k_args else (p_args if p_args else k_args)
            return await self.execute_dsl_function(dsl_func, inp)
        framework_log("ERROR", f"Function {name} not found", emoji="🤷")
        return None

    async def evaluate_expression(self, ops, ctx):
        if not ops: return None
        seed = await self.visit(ops[0], ctx)
        stages = [flow.step(lambda context=None: seed)]
        for op in ops[1:]:
            async def stage(context=None, _op=op):
                prev = context['outputs'][-1] if context and context.get('outputs') else seed
                if isinstance(_op, tuple) and _op[0] == 'CALL':
                    _, name, p_nodes, k_nodes = _op
                    p_args = [prev] + [await self.visit(a, ctx) for a in p_nodes]
                    k_args = {k: await self.visit(v, ctx) for k, v in k_nodes.items()}
                    return await self._execute(name, p_args, k_args)
                if isinstance(_op, (list, tuple)) and len(_op) == 3:
                    in_def, name, _ = _op
                    p_args = [prev] + [await self.visit(a, ctx) for a in (in_def if isinstance(in_def, (list, tuple)) else ([in_def] if in_def else []))]
                    return await self._execute(str(name), p_args, {})
                return await self._execute(str(_op), [prev], {})
            stage.__name__ = f"dsl_{str(op)[:20]}"
            stages.append(flow.step(stage))
        return await flow.pipe(*stages, context=(ctx or {}).copy())

    async def execute_dsl_function(self, func_def, args):
        in_def, body, out_def = func_def
        ctx = {}
        def get_p(p):
            if isinstance(p, tuple) and p[0] == 'TYPED': return (p[2], p[1])
            return (p.name if isinstance(p, DSLVariable) else str(p), None)
        def is_multi(d):
            if not isinstance(d, (list, tuple)) or not d: return False
            if len(d) == 3 and d[0] == 'TYPED': return False # Single typed param
            return True
        params = [get_p(p) for p in in_def] if is_multi(in_def) else [get_p(in_def)]
        arg_list = args if isinstance(args, (list, tuple)) and len(params) > 1 else [args]
        for (name, type_name), val in zip(params, arg_list):
            if type_name:
                t_map = {'int':int,'integer':int,'str':str,'string':str,'float':float,'number':(int,float),'bool':bool,'boolean':bool,'dict':dict,'list':list,'tuple':tuple}
                exp = t_map.get(type_name) or self.functions_map.get(type_name)
                if exp and not isinstance(val, exp): raise TypeError(f"Param {name} expected {type_name}, got {type(val)}")
            ctx[name] = val
        for k, v in body.items(): ctx[str(k)] = await self.visit(v, ctx)
        outs = [get_p(p)[0] for p in out_def] if is_multi(out_def) else [get_p(out_def)[0]]
        res = [ctx.get(o) for o in outs]
        return res[0] if len(res) == 1 else tuple(res)

async def _dsl_load_adapter(func_name, *args, **kw):
    import framework.service.context as context

    await asyncio.sleep(5)
    dd = context.container.module_cache()
    #print(func_name,dd)
    #return dd.get(f"framework/manager/{func_name}.py")
    return getattr(context.container, func_name)()

async def _dsl_load_service(func_name, *args, **kw):
        import framework.service.load as load
        func = getattr(load, func_name)
        try:
            # Se riceve un singolo dict come argomento posizionale, lo usa come kw
            if len(args) == 1 and isinstance(args[0], dict) and not kw:
                kw = args[0]
                args = ()
            
            if func_name == 'resource' and args and isinstance(args[0], dict) and 'path' in args[0]: args = (args[0]['path'],) + args[1:]
            res = await func(*args, **kw)
            return res.get('data', res) if isinstance(res, dict) else res
        except Exception as e:
            framework_log("ERROR", f"Error {func_name}: {e}", emoji="❌"); return None
    

dsl_functions = {
    n: lambda *a, n=n, **kw: _dsl_load_adapter(n, *a, **kw) 
    for n in ['storekeeper','messenger','executor','presenter','defender']
} | {
    n: lambda *a, n=n, **kw: _dsl_load_service(n, *a, **kw) 
    for n in ['resource','register']
}

# Proxy class per accedere ai metodi dell'executor con sintassi executor.method()
class ExecutorProxy:
    """Proxy that allows calling executor methods with dot notation in DSL."""
    
    def __init__(self):
        self._executor_instance = None
    
    async def _get_executor(self):
        """Lazy load executor instance."""
        if self._executor_instance is None:
            self._executor_instance = await _dsl_load_adapter('executor')
        return self._executor_instance
    
    async def all_completed(self, *args, **kwargs):
        """Wait for all tasks to complete."""
        executor = await self._get_executor()
        return await executor.all_completed(*args, **kwargs)
    
    async def first_completed(self, *args, **kwargs):
        """Return first task that completes successfully."""
        executor = await self._get_executor()
        return await executor.first_completed(*args, **kwargs)
    
    async def chain_completed(self, *args, **kwargs):
        """Execute tasks sequentially."""
        executor = await self._get_executor()
        return await executor.chain_completed(*args, **kwargs)
    
    async def together_completed(self, *args, **kwargs):
        """Fire and forget - start all tasks in background."""
        executor = await self._get_executor()
        return await executor.together_completed(*args, **kwargs)
    
    async def act(self, *args, **kwargs):
        """Execute action(s)."""
        executor = await self._get_executor()
        return await executor.act(*args, **kwargs)

# Create singleton instance
_executor_proxy = ExecutorProxy()

# Add executor proxy to dsl_functions
dsl_functions['executor'] = _executor_proxy

# Keep aliases for convenience
dsl_functions['all_completed'] = _executor_proxy.all_completed
dsl_functions['first_completed'] = _executor_proxy.first_completed
dsl_functions['chain'] = _executor_proxy.chain_completed
dsl_functions['sequential'] = _executor_proxy.chain_completed
dsl_functions['fire_and_forget'] = _executor_proxy.together_completed

# Helper to wrap a function as a step for use in switch/match
def _wrap_as_step(func):
    """Wraps a callable as a flow.step if it's not already a tuple."""
    if callable(func) and not isinstance(func, tuple):
        return flow.step(func)
    return func

# Custom switch that auto-wraps functions
async def _dsl_switch(cases_or_value, value_or_context=None, context=None):
    """Switch that auto-wraps callables as steps.
    
    Can be called as:
    - match(cases_dict, context) - standard call
    - value | match(cases_dict) - piped call where value becomes first arg
    """
    # Determine which argument is which
    if isinstance(cases_or_value, dict) and all(isinstance(k, str) for k in cases_or_value.keys()):
        # First arg is cases, second is context or value
        cases = cases_or_value
        if isinstance(value_or_context, dict) and '@' not in value_or_context:
            ctx = value_or_context
        else:
            # value_or_context is the value to match
            ctx = {'@': value_or_context} if value_or_context is not None else (context or {})
    else:
        # First arg is the value (from pipe), second is cases
        value = cases_or_value
        cases = value_or_context if isinstance(value_or_context, dict) else {}
        ctx = {'@': value}
    
    # Wrap actions as steps
    if isinstance(cases, dict):
        wrapped_cases = {k: _wrap_as_step(v) for k, v in cases.items()}
    elif isinstance(cases, (list, tuple)):
        if cases and isinstance(cases[0], (list, tuple)) and len(cases[0]) == 2:
            wrapped_cases = [(cond, _wrap_as_step(action)) for cond, action in cases]
        else:
            wrapped_cases = cases
    else:
        wrapped_cases = cases
    
    return await flow.switch(wrapped_cases, ctx)

dsl_functions.update({
    'format': flow.format, 'foreach': flow.foreach, 'convert': flow.convert, 'get': flow.get,
    'keys': lambda d: list(d.keys()) if isinstance(d, dict) else [],
    'values': lambda d: list(d.values()) if isinstance(d, dict) else [],
    'items': lambda d: list(d.items()) if isinstance(d, dict) else [],
    'print': lambda d: (print(f"*** CUSTOM PRINT ***: {d}"), d)[1],
    'pick': lambda d, keys: {k: v for k, v in d.items() if k in keys} if isinstance(d, dict) and isinstance(keys, (list, tuple)) else d,
    'filter': lambda d, keys: {k: v for k, v in d.items() if k in keys} if isinstance(d, dict) and isinstance(keys, (list, tuple)) else d,
    'switch': _dsl_switch, 'match': _dsl_switch,
    'batch': flow.batch, 'parallel': flow.batch,
    'race': flow.race, 'timeout': flow.timeout, 'throttle': flow.throttle,
    'catch': flow.catch, 'branch': flow.branch, 'retry': flow.retry,
    'fallback': flow.fallback,
    'project': lambda d, m=None: (
        (lambda data, template: (
            (lambda res_func: (
                [res_func(res_func, i, template) for i in data] if isinstance(data, list) else res_func(res_func, data, template)
            ))(
                lambda self, i, m: (
                    {k: self(self, i, v) for k, v in m.items()} if isinstance(m, dict) else
                    [self(self, i, x) for x in m] if isinstance(m, list) else
                    (flow.get(i, m[2:]) if isinstance(m, str) and m.startswith("@.") else 
                     (re.sub(r'\{([^{}]+)\}', lambda match: str(flow.get(i, match.group(1), match.group(0))), m) if isinstance(m, str) and "{" in m and "}" in m else m))
                )
            )
        ))(d, m) if m is not None else d
    ),
    'transform': lambda d, m=None: (
        (lambda data, template: (
            (lambda res_func: (
                [res_func(res_func, i, template) for i in data] if isinstance(data, list) else res_func(res_func, data, template)
            ))(
                lambda self, i, m: (
                    {k: self(self, i, v) for k, v in m.items()} if isinstance(m, dict) else
                    [self(self, i, x) for x in m] if isinstance(m, list) else
                    (flow.get(i, m[2:]) if isinstance(m, str) and m.startswith("@.") else 
                     (re.sub(r'\{([^{}]+)\}', lambda match: str(flow.get(i, match.group(1), match.group(0))), m) if isinstance(m, str) and "{" in m and "}" in m else m))
                )
            )
        ))(d, m) if m is not None else d
    ),
    'map': lambda d, f: [mistql.query(f, data=i) for i in d] if isinstance(d, list) else d,
    'remap': lambda data, *names: [dict(zip(names, item)) for item in data] if isinstance(data, (list, tuple)) else data,
    'entries': lambda d: list(d.items()) if isinstance(d, dict) else [],
    'merge': lambda a, b: (
        (a | b) if isinstance(a, dict) and isinstance(b, dict) else 
        ((list(a) if isinstance(a, (list, tuple)) else [a]) + (list(b) if isinstance(b, (list, tuple)) else [b]))
    ),
    'concat': lambda a, b: ((list(a) if isinstance(a, (list, tuple)) else [a]) + (list(b) if isinstance(b, (list, tuple)) else [b])),
    'query': lambda data, q: mistql.query(q, data=data),
    **{k: v for k, v in zip(['dict','list','str','int','float','bool'], [dict,list,str,int,float,bool])},
    'integer':int,'string':str,'boolean':bool,'number':float,'relative':int,'natural':int,'rational':float,'complex':float
})

def parse_dsl_file(content):
    return ConfigTransformer().transform(Lark(grammar, parser='earley').parse(content))

async def execute_dsl_file(content):
    return await DSLVisitor(dsl_functions).run(parse_dsl_file(content))

async def run_dsl_tests(visitor, parsed_data):
    test_suite = parsed_data.get('test_suite', [])
    if isinstance(test_suite, dict): test_suite = [test_suite]
    if not isinstance(test_suite, (list, tuple)): return False
    all_passed = True
    print("\n" + "="*40 + f"\nDSL Tests: {len(test_suite)}\n" + "="*40)
    for test in test_suite:
        if not isinstance(test, dict): continue
        target, args, expected = test.get('target'), test.get('input_args'), test.get('expected_output')
        print(f"Testing '{target}'...", end=" ")
        try:
            target_def = parsed_data.get(target)
            actual = await visitor.execute_dsl_function(target_def, args) if isinstance(target_def, tuple) and len(target_def) == 3 else await visitor.visit(target_def)
            if actual == expected: print("🟢 OK")
            else: print(f"🔴 FAILED (expected {expected}, got {actual})"); all_passed = False
        except Exception as e:
            # import traceback
            # traceback.print_exc()
            print(f"🔴 EXC: {e}"); all_passed = False
    print("="*40 + f"\nRESULT: {'🟢 PASSED' if all_passed else '🔴 FAILED'}\n" + "="*40)
    return all_passed
