"""
DSL Language Interpreter
========================
Complete & faithful version
- NO global parser
- TriggerEngine separated
- Fully compatible with original DSL
"""

import asyncio
import inspect
import operator

from lark import Lark, Transformer, Token

import framework.service.scheme as scheme
import framework.service.flow as flow
import framework.service.load as load


# ============================================================================
# GRAMMAR
# ============================================================================

GRAMMAR = r"""
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
        | ("true"i | "True"i) -> true 
        | ("false"i | "False"i) -> false 
        | ANY -> any_val
    
    ?power_expr: atom | power_expr POW_OP atom -> power
    ?mult_expr: power_expr | mult_expr (MUL_OP | DIV_OP | MOD_OP) power_expr -> binary_op
    ?add_expr: mult_expr | add_expr (ADD_OP | SUB_OP) mult_expr -> binary_op
    ?pipe_expr: add_expr (PIPE (add_expr | tuple_inline))* -> pipe_node
    ?comparison_expr: pipe_expr | comparison_expr COMPARISON_OP pipe_expr -> binary_op
    ?not_expr: comparison_expr | "not" not_expr -> not_op
    ?and_expr: not_expr | and_expr ("and" | "&") not_expr -> and_op
    ?or_expr: and_expr | or_expr ("or" | "|") and_expr -> or_op
    ?expression: or_expr
    
    dictionary: braced_dict
    ?item: pair | function_call
    typed_name: CNAME ":" (CNAME | QUALIFIED_CNAME) -> typed_name_node
    declaration: typed_name ":=" (expression | tuple_inline)
    mapping: (value | tuple_inline | typed_name | function_call | CNAME | QUALIFIED_CNAME) ":" (expression | tuple_inline)
    ?pair: "(" (declaration | mapping) ")" | (declaration | mapping)
    
    ?valid_tuple_item: value | dictionary | tuple | list | "(" expression ")" | typed_name | CNAME | QUALIFIED_CNAME
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

# ============================================================================
# ERRORS
# ============================================================================

class DSLError(Exception):
    pass


# ============================================================================
# OPS / TYPES
# ============================================================================

OPS_MAP = {
    '+':'ADD','-':'SUB','*':'MUL','/':'DIV','%':'MOD','^':'POW',
    '==':'EQ','!=':'NEQ','>=':'GTE','<=':'LTE','>':'GT','<':'LT'
}

OPS_FUNCTIONS = {
    'OP_ADD': operator.add, 'OP_SUB': operator.sub,
    'OP_MUL': operator.mul, 'OP_DIV': operator.truediv,
    'OP_MOD': operator.mod, 'OP_POW': operator.pow,
    'OP_EQ': operator.eq, 'OP_NEQ': operator.ne,
    'OP_GT': operator.gt, 'OP_LT': operator.lt,
    'OP_GTE': operator.ge, 'OP_LTE': operator.le,
    'OP_AND': lambda a, b: a and b,
    'OP_OR': lambda a, b: a or b,
    'OP_NOT': lambda a: not a,
}

TYPE_MAP = {
    'int': int, 'float': float, 'str': str, 'bool': bool,
    'dict': dict, 'list': list, 'any': object, 'type': dict
}

CUSTOM_TYPES = {}

DSL_FUNCTIONS = {
    'resource': load.resource,
    'transform': scheme.transform,
    'normalize': scheme.normalize,
    'put': scheme.put,
    'format': scheme.format,
    'foreach': flow.foreach,
    #'batch': flow.batch,
    #'parallel': flow.batch,
    #'race': flow.race,
    #'timeout': flow.timeout,
    #'throttle': flow.throttle,
    'retry': flow.retry,
    #'fallback': flow.fallback,
    'keys': lambda d: list(d.keys()) if isinstance(d, dict) else [],
    'values': lambda d: list(d.values()) if isinstance(d, dict) else [],
    'print': lambda d: (print(d), d)[1],
}


# ============================================================================
# AST HELPERS
# ============================================================================

is_var = lambda n: isinstance(n, tuple) and n[:1] == ('VAR',)
is_typed = lambda n: isinstance(n, tuple) and n[:1] == ('TYPED',)
is_call = lambda n: isinstance(n, tuple) and n[:1] == ('CALL',)
is_expression = lambda n: isinstance(n, tuple) and n[:1] == ('EXPRESSION',)
is_function_def = lambda n: isinstance(n, tuple) and len(n) == 3 and isinstance(n[1], dict)
is_trigger = lambda n: is_call(n) or (isinstance(n, tuple) and '*' in n)

get_name = lambda n: n[1] if is_var(n) else n[2] if is_typed(n) else str(n)
get_type = lambda n: n[1] if is_typed(n) else None

def unwrap(v):
    return v.get('outputs') if isinstance(v, dict) and 'outputs' in v else v


# ============================================================================
# TRANSFORMER (IDENTICO ALL'ORIGINALE)
# ============================================================================

class DSLTransformer(Transformer):
    start = lambda self, i: i[0] if i else {}
    call_args = lambda self, a: a
    arg_pos = lambda self, a: ('POS', a[0])
    arg_kw = lambda self, a: ('KW', str(a[0]), a[1])
    declaration = lambda self, a: (a[0], a[1])
    mapping = lambda self, a: (a[0], a[1])
    pair = lambda self, a: a[0]
    binary_op = lambda self, a: (f'OP_{OPS_MAP[str(a[1])]}', a[0], a[2])
    power = lambda self, a: ('OP_POW', a[0], a[2])
    and_op = lambda self, a: ('OP_AND', a[0], a[-1])
    or_op = lambda self, a: ('OP_OR', a[0], a[-1])
    not_op = lambda self, a: ('OP_NOT', a[-1], None)
    number = lambda self, n: float(n[0]) if '.' in str(n[0]) else int(n[0])
    string = lambda self, s: str(s[0]).strip('"\'')
    true = lambda self, _: True
    false = lambda self, _: False
    any_val = lambda self, _: '*'
    typed_name_node = lambda self, a: ('TYPED', str(a[0]), str(a[1]))
    def pipe_node(self, items):
        nodes = [x for x in items if not isinstance(x, Token)]
        return nodes[0] if len(nodes) == 1 else ('EXPRESSION', nodes)
    def function_call(self, a):
        name = get_name(a[0])
        args = a[1] if len(a) > 1 else []
        return ('CALL', name,
                tuple(x[1] for x in args if x[0] == 'POS'),
                {x[1]: x[2] for x in args if x[0] == 'KW'})
    def dictionary(self, items):
        result, triggers = {}, []
        for i in items:
            if isinstance(i, dict):
                result.update(i)
            elif isinstance(i, tuple) and len(i) == 2:
                k, v = i
                result[get_name(k) if is_typed(k) else str(k)] = v
            elif is_call(i):
                result[f'__stmt_{i[1]}'] = i
        return result
    def tuple_(self, i):
        i = [x for x in i if x is not None]
        return tuple(i) if len(i) > 1 else i[0]
    def list_(self, i):
        return [x for x in i if x is not None]
    def simple_key(self, s):
        v = s[0] if isinstance(s, list) else s
        return v if isinstance(v, tuple) else ('VAR', str(v))


# ============================================================================
# TYPE VALIDATION
# ============================================================================

async def validate_type(value, type_name, var_name):
    if type_name in CUSTOM_TYPES:
        return await scheme.normalize(value, CUSTOM_TYPES[type_name])

    py_type = TYPE_MAP.get(type_name)
    if not py_type or py_type is object:
        return value

    if py_type is int and isinstance(value, float) and value.is_integer():
        return int(value)

    if not isinstance(value, py_type):
        raise DSLError(
            f"Type error in '{var_name}': expected {type_name}, got {type(value).__name__}"
        )

    return value


# ============================================================================
# TRIGGER ENGINE (SEPARATO)
# ============================================================================

class TriggerEngine:

    def __init__(self, visitor):
        self.visitor = visitor
        self.tasks = []

    def register(self, triggers, ctx):
        for trigger, action in triggers:
            if is_call(trigger):
                task = asyncio.create_task(self._event_loop(trigger, action, ctx))
            else:
                task = asyncio.create_task(self._cron_loop(trigger, action, ctx))
            self.tasks.append(task)

    async def _event_loop(self, call_node, action, ctx):
        framework_log("INFO", f"Event listener: {call_node[1]}", emoji="👂")
        while True:
            try:
                result = await self.visitor.visit(call_node, ctx)
                if isinstance(result, dict) and result.get('success'):
                    await self.visitor.visit(action, {**ctx, '@event': result.get('data')})
                else:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(5)

    async def _cron_loop(self, pattern, action, ctx):
        import datetime
        framework_log("INFO", f"Cron trigger: {pattern}", emoji="⏰")
        while True:
            now = datetime.datetime.now()
            cur = (now.minute, now.hour, now.day, now.month, now.weekday())
            if all(p == '*' or str(p) == str(c) for p, c in zip(pattern, cur)):
                await self.visitor.visit(action, ctx)
            await asyncio.sleep(60 - now.second)

    async def shutdown(self):
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

# ============================================================================
# DSL VISITOR (COMPLETO)
# ============================================================================

class Interpreter:

    def __init__(self, functions=None):
        self.functions = functions or {}
        self.root = {}
        self.triggers = TriggerEngine(self)

    async def visit(self, node, ctx=None):
        ctx = ctx or {}

        if node is None or isinstance(node, (int, float, str, bool)):
            return node

        if isinstance(node, list):
            return [unwrap(await self.visit(x, ctx)) for x in node]

        if isinstance(node, dict):
            local = {**ctx, '__execute_dsl_function__': self.execute_dsl_function}
            out = {}

            for k, v in node.items():
                if k == '__triggers__':
                    continue

                val = unwrap(await self.visit(v, local))

                out[str(k)] = val
                local[str(k)] = val

            if '__triggers__' in node:
                self.triggers.register(node['__triggers__'], local)

            return out

        if isinstance(node, tuple):
            tag = node[0]

            if tag in OPS_FUNCTIONS:
                args = [unwrap(await self.visit(x, ctx)) for x in node[1:]]
                return OPS_FUNCTIONS[tag](*args)

            if tag == 'VAR':
                return await self.resolve(node[1], ctx)

            if tag == 'TYPED':
                return await self.resolve(node[2], ctx)

            if tag == 'CALL':
                return await self.execute_call(node, ctx)

            if tag == 'EXPRESSION':
                return await self.evaluate_expression(node[1], ctx)

            results = []
            for x in node:
                r = await self.visit(x, ctx)
                results.append(unwrap(r))
            return tuple(results)

        return node

    async def resolve(self, name, ctx):
        if '.' in name:
            return await self.resolve_path(name, ctx)

        for scope in (ctx, self.functions, self.root):
            if scope and name in scope:
                return scope[name]

        return TYPE_MAP.get(name, name)

    async def resolve_path(self, path, ctx):
        parts = path.split('.')
        obj = await self.resolve(parts[0], ctx)
        for p in parts[1:]:
            obj = obj.get(p) if isinstance(obj, dict) else getattr(obj, p, None)
        return obj

    async def execute_call(self, node, ctx):
        name, pos, kw = node[1], node[2], node[3]
        fn = await self.resolve(name, ctx)

        args = [unwrap(await self.visit(x, ctx)) for x in pos]
        kwargs = {k: unwrap(await self.visit(v, ctx)) for k, v in kw.items()}

        if is_function_def(fn):
            return await self.execute_dsl_function(fn, args, kwargs)

        result = fn(*args, **kwargs)
        return await result if asyncio.iscoroutine(result) else result

    async def evaluate_expression(self, ops, ctx):
        val = unwrap(await self.visit(ops[0], ctx))
        for step in ops[1:]:
            if is_call(step):
                val = unwrap(await self.execute_call(
                    ('CALL', step[1], (val,) + step[2], step[3]), ctx
                ))
            else:
                fn = await self.resolve(get_name(step), ctx)
                val = unwrap(await fn(val))
        return val

    async def execute_dsl_function(self, fn, args, kwargs=None):
        kwargs = kwargs or {}
        inputs, body, outputs = fn

        local = {}
        for i, p in enumerate(inputs):
            name, t = get_name(p), get_type(p)
            v = args[i] if i < len(args) else kwargs.get(name)
            if t:
                v = await validate_type(v, t, name)
            local[name] = v

        result = unwrap(await self.visit(body, local))
        if isinstance(result, dict):
            local.update(result)

        outs = [local[get_name(o)] for o in outputs]
        return outs[0] if len(outs) == 1 else tuple(outs)

    @flow.action()
    async def run(self, ast,**constants):
        self.root = ast
        result = await self.visit(ast)
        await self.triggers.shutdown()
        return unwrap(result)


# ============================================================================
# PUBLIC API (NO GLOBAL PARSER)
# ============================================================================

def create_parser():
    return Lark(GRAMMAR, parser='earley')

@flow.action()
def parse(content: str, parser: Lark,**data):
    return DSLTransformer().transform(parser.parse(content))

@flow.action()
async def execute(content_or_ast, parser, functions):
    ast = parse(content_or_ast, parser) if isinstance(content_or_ast, str) else content_or_ast
    return await Interpreter(functions).run(ast)