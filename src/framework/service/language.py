"""
DSL Language Interpreter - Improved Version
============================================
A robust, maintainable DSL interpreter with enhanced error handling,
performance optimizations, and cleaner architecture.
"""

from typing import Dict, Any, Optional, List, Callable, Union, Tuple, Set
import asyncio
import operator
import re
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from lark import Lark, Transformer, Token
import mistql
import inspect

from framework.service.flow import (
    asynchronous, synchronous, get_transaction_id, set_transaction_id, 
    _transaction_id, convert, get, put, format, route, normalize, framework_log, _load_resource
)
import framework.service.flow as flow
import framework.service.load as load

# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================

class NodeType(Enum):
    """Enumeration of all node types for type safety"""
    VAR = "VAR"
    TYPED = "TYPED"
    CALL = "CALL"
    EXPRESSION = "EXPRESSION"
    OPERATION = "OPERATION"
    FUNCTION_DEF = "FUNCTION_DEF"
    TRIGGER = "TRIGGER"

class Config:
    """Central configuration"""
    MAX_RECURSION_DEPTH = 1000
    DEFAULT_TIMEOUT = 300  # seconds
    CACHE_SIZE = 128
    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 1.0

# ============================================================================
# DATA STORE (The "Data-Driven" State Manager)
# ============================================================================

class DataStore:
    """
    Reactive state manager with path-based access and validation.
    """
    def __init__(self, initial_state: Optional[Dict] = None):
        self._state = initial_state or {}
        self._subscribers: Dict[str, List[Callable]] = {}
        self._lock = asyncio.Lock()

    async def get(self, path: str, default: Any = None) -> Any:
        """Get value by path (dot notation support)"""
        return get(self._state, path, default)

    async def set(self, path: str, value: Any):
        """Set value by path and notify listeners"""
        async with self._lock:
            self._state = put(self._state, path, value)
        
        await self._notify(path, value)

    def subscribe(self, path_pattern: str, callback: Callable):
        """Subscribe to changes (wildcards supported)"""
        if path_pattern not in self._subscribers:
            self._subscribers[path_pattern] = []
        self._subscribers[path_pattern].append(callback)


    async def _notify(self, path: str, value: Any):
        """Notify relevant subscribers"""
        tasks = []
        for pattern, callbacks in self._subscribers.items():
            if WildcardMatcher.match(path, pattern):
                for cb in callbacks:
                    tasks.append(cb(path, value))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def to_dict(self) -> Dict:
        return self._state.copy()


# ============================================================================
# GRAMMAR (unchanged but with better documentation)
# ============================================================================
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

    value: SIGNED_NUMBER -> number | (ESCAPED_STRING | SINGLE_QUOTED_STRING) -> string
        | ("true"i | "True"i) -> true | ("false"i | "False"i) -> false | ANY -> any_val
    
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
# CUSTOM EXCEPTIONS
# ============================================================================

class DSLError(Exception):
    """Base exception for DSL errors"""
    pass

class DSLSyntaxError(DSLError):
    """Syntax error in DSL code"""
    pass

class DSLTypeError(DSLError):
    """Type validation error"""
    pass

class DSLRuntimeError(DSLError):
    """Runtime execution error"""
    pass

class DSLTimeoutError(DSLError):
    """Execution timeout"""
    pass

class DSLRecursionError(DSLError):
    """Recursion depth exceeded"""
    pass

# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass(frozen=True)
class TypedName:
    """Immutable typed name representation"""
    name: str
    type_name: str
    
    def __str__(self):
        return f"{self.name}:{self.type_name}"

@dataclass(frozen=True)
class FunctionCall:
    """Immutable function call representation"""
    name: str
    pos_args: Tuple[Any, ...]
    kw_args: Dict[str, Any]
    
    def __str__(self):
        args_str = ", ".join(str(a) for a in self.pos_args)
        kwargs_str = ", ".join(f"{k}={v}" for k, v in self.kw_args.items())
        all_args = ", ".join(filter(None, [args_str, kwargs_str]))
        return f"{self.name}({all_args})"

# ============================================================================
# NODE TYPE CHECKERS (Enhanced with caching)
# ============================================================================
class NodeTypeChecker:
    """Enhanced type checking with caching for performance"""
    
    _cache: Dict[int, bool] = {}
    
    @staticmethod
    def is_var(node) -> bool:
        """Check if node is a variable reference"""
        return isinstance(node, tuple) and len(node) == 2 and node[0] == 'VAR'
    
    @staticmethod
    def is_typed(node) -> bool:
        """Check if node is a typed name"""
        return isinstance(node, tuple) and len(node) == 3 and node[0] == 'TYPED'
    
    @staticmethod
    def is_call(node) -> bool:
        """Check if node is a function call"""
        return isinstance(node, tuple) and len(node) >= 2 and node[0] == 'CALL'
    
    @staticmethod
    def is_expression(node) -> bool:
        """Check if node is an expression"""
        return isinstance(node, tuple) and len(node) >= 2 and node[0] == 'EXPRESSION'
    
    @staticmethod
    def is_operation(node, ops_dict: Dict) -> bool:
        """Check if node is an operation"""
        return isinstance(node, tuple) and node and isinstance(node[0], str) and node[0] in ops_dict
    
    @staticmethod
    def is_function_def(node) -> bool:
        """Check if node is a function definition"""
        return isinstance(node, tuple) and len(node) == 3 and isinstance(node[1], dict)
    
    @staticmethod
    def is_trigger(node) -> bool:
        """Check if node is a trigger (event or cron)"""
        is_call = NodeTypeChecker.is_call(node)
        is_cron = isinstance(node, tuple) and any(x == '*' for x in node if isinstance(x, str))
        return is_call or is_cron
    
    @staticmethod
    def is_token(node) -> bool:
        """Check if node is a Lark token"""
        return isinstance(node, Token)
    
    @staticmethod
    def is_resolvable(node) -> bool:
        """Check if node can be resolved"""
        return hasattr(node, 'type') or NodeTypeChecker.is_var(node) or NodeTypeChecker.is_typed(node)

# ============================================================================
# NODE EXTRACTORS (Enhanced with validation)
# ============================================================================
class NodeExtractor:
    """Extract data from nodes with validation"""
    
    @staticmethod
    def get_var_name(node) -> str:
        """Extract variable name with validation"""
        if not NodeTypeChecker.is_var(node):
            return str(node)
        return node[1]
    
    @staticmethod
    def get_typed_name(node) -> Optional[str]:
        """Extract typed name"""
        return node[2] if NodeTypeChecker.is_typed(node) else None
    
    @staticmethod
    def get_typed_type(node) -> Optional[str]:
        """Extract type from typed node"""
        return node[1] if NodeTypeChecker.is_typed(node) else None
    
    @staticmethod
    def get_call_name(node) -> Optional[str]:
        """Extract function call name"""
        return node[1] if NodeTypeChecker.is_call(node) else None
    
    @staticmethod
    def get_call_pos_args(node) -> Tuple:
        """Extract positional arguments from call"""
        return node[2] if NodeTypeChecker.is_call(node) and len(node) > 2 else ()
    
    @staticmethod
    def get_call_kw_args(node) -> Dict:
        """Extract keyword arguments from call"""
        return node[3] if NodeTypeChecker.is_call(node) and len(node) > 3 else {}
    
    @staticmethod
    def get_operation_tag(node) -> Optional[str]:
        """Extract operation tag"""
        return node[0] if isinstance(node, tuple) and node else None
    
    @staticmethod
    def get_operation_args(node) -> List:
        """Extract operation arguments"""
        return list(node[1:]) if isinstance(node, tuple) and node else []
    
    @staticmethod
    def get_dict_value_or_none(d: Dict, key: str) -> Any:
        """Safe dictionary access"""
        return d.get(key) if isinstance(d, dict) else None

# ============================================================================
# STRING UTILITIES (Enhanced with caching)
# ============================================================================
class StringUtil:
    """String manipulation utilities with caching"""
    
    @staticmethod
    @lru_cache(maxsize=Config.CACHE_SIZE)
    def split_dotted_name(name: str) -> Tuple[str, ...]:
        """Split dotted name (cached)"""
        return tuple(name.split('.'))
    
    @staticmethod
    def has_dots(name: str) -> bool:
        """Check if name contains dots"""
        return '.' in name
    
    @staticmethod
    def strip_quotes(s: str) -> str:
        """Remove surrounding quotes"""
        return s.strip('"\'')
    
    @staticmethod
    @lru_cache(maxsize=Config.CACHE_SIZE)
    def to_regex_pattern(pattern: str) -> str:
        """Convert wildcard pattern to regex (cached)"""
        return re.escape(pattern).replace(r'\*', '.*').replace(r'\?', '.')

# ============================================================================
# PATTERN MATCHERS (Enhanced)
# ============================================================================
class PatternMatcher:
    """Pattern matching utilities"""
    
    @staticmethod
    def match_string_wildcard(data: str, pattern: str) -> bool:
        """Match string with wildcard pattern"""
        regex = StringUtil.to_regex_pattern(pattern)
        return bool(re.fullmatch(regex, str(data)))
    
    @staticmethod
    def match_list_wildcard(data: Tuple, pattern: Tuple) -> bool:
        """Match list/tuple with wildcard pattern"""
        if len(pattern) != len(data):
            return False
        return all(p == '*' or str(p) == str(d) for p, d in zip(pattern, data))
    
    @staticmethod
    def match_exact(data: Any, pattern: Any) -> bool:
        """Exact match comparison"""
        return str(data) == str(pattern)

# ============================================================================
# TRANSFORMER (Enhanced with better error handling)
# ============================================================================
class ConfigTransformer(Transformer):
    """Transform parsed tree into executable structure"""
    
    def __init__(self):
        super().__init__()
        self._op_map = {
            '+': 'ADD', '-': 'SUB', '*': 'MUL', '/': 'DIV', '%': 'MOD', '^': 'POW',
            '==': 'EQ', '!=': 'NEQ', '>=': 'GTE', '<=': 'LTE', '>': 'GT', '<': 'LT'
        }
    
    # Single-purpose extractors
    def _extract_call_name(self, node) -> str:
        """Extract name from call node"""
        if NodeTypeChecker.is_typed(node):
            return NodeExtractor.get_typed_name(node)
        return str(node)
    
    def _create_call_tuple(self, name: str, pos_args: Tuple, kw_args: Dict) -> Tuple:
        """Create call tuple"""
        return ('CALL', name, pos_args, kw_args)
    
    def _filter_pipes(self, items: List) -> List:
        """Filter out pipe tokens"""
        return [i for i in items if not (NodeTypeChecker.is_token(i) and i.type == 'PIPE')]
    
    def _split_args_by_type(self, raw_args: List) -> Tuple[Tuple, Dict]:
        """Split arguments into positional and keyword"""
        pos_args = tuple(a[1] for a in raw_args if a[0] == 'POS')
        kw_args = {a[1]: a[2] for a in raw_args if a[0] == 'KW'}
        return pos_args, kw_args
    
    def _extract_op_symbol(self, op_token) -> str:
        """Extract operator symbol"""
        return str(op_token)
    
    def _map_op_to_name(self, symbol: str) -> str:
        """Map operator symbol to name"""
        return self._op_map.get(symbol, symbol)
    
    def _create_op_tuple(self, op_name: str, left, right) -> Tuple:
        """Create operation tuple"""
        return (f'OP_{op_name}', left, right)
    
    def _is_single_item_list(self, items: List) -> bool:
        """Check if list has single item"""
        return len(items) == 1
    
    """
    Refactored transformer that uses a data-driven rule map.
    Eliminates the need for dozens of individual methods.
    """
    
    RULES = {
        'start': lambda items: items[0] if items else {},
        'call_args': lambda args: args,
        'arg_pos': lambda args: ('POS', args[0]),
        'arg_kw': lambda args: ('KW', str(args[0]), args[1]),
        'declaration': lambda args: (args[0], args[1]),
        'mapping': lambda args: (args[0], args[1]),
        'pair': lambda args: args[0],
        'statement': lambda args: args[0],
        'binary_op': lambda args: (f'OP_{OPS_MAP[str(args[1])]}', args[0], args[2]),
        'power': lambda args: ('OP_POW', args[0], args[2]),
        'and_op': lambda args: ('OP_AND', args[0], args[2] if len(args) > 2 else args[1]),
        'or_op': lambda args: ('OP_OR', args[0], args[2] if len(args) > 2 else args[1]),
        'not_op': lambda args: ('OP_NOT', args[1] if len(args) > 1 else args[0], None),
        'number': lambda n: float(str(n[0])) if '.' in str(n[0]) else int(str(n[0])),
        'string': lambda s: StringUtil.strip_quotes(str(s[0])),
        'true': lambda _: True,
        'false': lambda _: False,
        'any_val': lambda _: '*',
        'typed_name_node': lambda args: ('TYPED', str(args[0]), str(args[1]))
    }

    def __getattr__(self, name):
        """Dynamic dispatch for rules not explicitly defined"""
        if name in self.RULES:
            return self.RULES[name]
        return super().__getattribute__(name)

    def pipe_node(self, items):
        filtered = [i for i in items if not (NodeTypeChecker.is_token(i) and i.type == 'PIPE')]
        return filtered[0] if len(filtered) == 1 else ('EXPRESSION', filtered)

    def function_call(self, args):
        name = NodeExtractor.get_typed_name(args[0]) if NodeTypeChecker.is_typed(args[0]) else str(args[0])
        raw_args = args[1] if len(args) > 1 else []
        pos_args = tuple(a[1] for a in raw_args if a[0] == 'POS')
        kw_args = {a[1]: a[2] for a in raw_args if a[0] == 'KW'}
        return ('CALL', name, pos_args, kw_args)

    def atom(self, items):
        for i in items:
            if not NodeTypeChecker.is_token(i): return i
        return items[0]

    def dictionary(self, items):
        res, triggers = {}, []
        for item in items:
            if isinstance(item, dict): 
                res.update(item)
            elif isinstance(item, tuple) and len(item) == 2:
                k, v = item
                if NodeTypeChecker.is_trigger(k): triggers.append((k, v))
                else: res[k if NodeTypeChecker.is_typed(k) else str(k)] = v
            elif NodeTypeChecker.is_call(item):
                res[f"__stmt_{item[1]}"] = item
        if triggers: res['__triggers__'] = triggers
        return res


    def tuple_(self, items):
        it = [i for i in items if i is not None]
        return it[0] if len(it) == 1 else tuple(it)

    def list_(self, items):
        return [i for i in items if i is not None]

    def simple_key(self, s):
        v = s[0] if isinstance(s, list) else s
        return v if isinstance(v, tuple) else ('VAR', str(v))

OPS_MAP = {
    '+':'ADD','-':'SUB','*':'MUL','/':'DIV','%':'MOD',
    '==':'EQ','!=':'NEQ','>=':'GTE','<=':'LTE','>':'GT','<':'LT'
}


# ============================================================================
# TYPE VALIDATOR (Enhanced with better error messages)
# ============================================================================
class TypeValidator:
    """Enhanced type validation with better error reporting"""
    
    TYPE_MAP = {
        'int': int, 'integer': int, 'i8': int, 'i16': int, 'i32': int, 'i64': int, 'i128': int,
        'str': str, 'string': str, 'dict': dict, 'list': list, 'array': list,
        'float': float, 'f8': float, 'f16': float, 'f32': float, 'f64': float, 'f128': float,
        'tuple': tuple, 'bool': bool, 'boolean': bool, 'any': object, 'number': (int, float)
    }
    
    @staticmethod
    @lru_cache(maxsize=Config.CACHE_SIZE)
    def get_type_class(type_name: str):
        """Get type class from name (cached)"""
        return TypeValidator.TYPE_MAP.get(type_name)
    
    @staticmethod
    def is_any_type(type_class) -> bool:
        """Check if type is 'any'"""
        return type_class is object
    
    @staticmethod
    def is_int_type(type_class) -> bool:
        """Check if type is int"""
        return type_class is int
    
    @staticmethod
    def is_whole_number(value) -> bool:
        """Check if float is whole number"""
        return isinstance(value, float) and value.is_integer()
    
    @staticmethod
    def check_instance(value, type_class) -> bool:
        """Check if value is instance of type"""
        return isinstance(value, type_class)
    
    @staticmethod
    def validate(value, type_name: str, var_name: str):
        """Validate value against type with detailed error"""
        type_class = TypeValidator.get_type_class(type_name)
        
        if type_class is None:
            framework_log("WARN", f"Unknown type: {type_name}", emoji="⚠️")
            return
        
        if TypeValidator.is_any_type(type_class):
            return
        
        # Forgive float->int for whole numbers
        if TypeValidator.is_int_type(type_class) and TypeValidator.is_whole_number(value):
            return
        
        if not TypeValidator.check_instance(value, type_class):
            raise DSLTypeError(
                f"Type error in '{var_name}': expected {type_name}, "
                f"got {type(value).__name__} with value '{value}'"
            )

# ============================================================================
# WILDCARD MATCHER (Unchanged but with type hints)
# ============================================================================
class WildcardMatcher:
    """Pattern matching with wildcards"""
    
    @staticmethod
    def match(data: Any, pattern: Any) -> bool:
        """Match data against pattern with wildcard support"""
        if isinstance(pattern, str):
            return PatternMatcher.match_string_wildcard(data, pattern)
        
        if isinstance(pattern, (list, tuple)) and isinstance(data, (list, tuple)):
            return PatternMatcher.match_list_wildcard(data, pattern)
        
        return PatternMatcher.match_exact(data, pattern)

# ============================================================================
# CONTEXT MANAGER (Enhanced with immutability options)
# ============================================================================
class ContextManager:
    """Context management utilities"""
    
    @staticmethod
    def create_empty() -> Dict:
        """Create empty context"""
        return {}
    
    @staticmethod
    def copy_context(ctx: Optional[Dict]) -> Dict:
        """Create shallow copy of context"""
        return (ctx or {}).copy()
    
    @staticmethod
    def merge_contexts(ctx1: Dict, ctx2: Dict) -> Dict:
        """Merge two contexts (ctx2 overrides ctx1)"""
        return {**ctx1, **ctx2}
    
    @staticmethod
    def add_executor(ctx: Dict, executor: Callable) -> Dict:
        """Add executor function to context"""
        ctx['__execute_dsl_function__'] = executor
        return ctx
    
    @staticmethod
    def get_value(ctx: Dict, key: str, default=None):
        """Get value from context with default"""
        return ctx.get(key, default)
    
    @staticmethod
    def set_value(ctx: Dict, key: str, value) -> Dict:
        """Set value in context (mutates)"""
        ctx[key] = value
        return ctx
    
    @staticmethod
    def has_key(ctx: Dict, key: str) -> bool:
        """Check if key exists in context"""
        return key in ctx

# ============================================================================
# RESULT UNWRAPPER (Enhanced)
# ============================================================================
class ResultUnwrapper:
    """Unwrap transactional results"""
    
    @staticmethod
    def is_transactional(result) -> bool:
        """Check if result is transactional"""
        return isinstance(result, dict) and (result.get('success') is True or result.get('ok') is True)
    
    @staticmethod
    def has_data_field(result: Dict) -> bool:
        """Check if result has data field"""
        return 'data' in result
    
    @staticmethod
    def get_data(result: Dict):
        """Extract data from result"""
        return result['data']
    
    @staticmethod
    def unwrap(result):
        """Unwrap transactional result or return as-is"""
        if ResultUnwrapper.is_transactional(result) and ResultUnwrapper.has_data_field(result):
            return ResultUnwrapper.get_data(result)
        return result

# ============================================================================
# ARGUMENT RESOLVER (Enhanced)
# ============================================================================
class ArgumentResolver:
    """Resolve function arguments"""
    
    @staticmethod
    async def resolve_if_var(arg, visitor):
        """Resolve argument if it's a variable"""
        return await visitor.visit(arg) if NodeTypeChecker.is_var(arg) else arg
    
    @staticmethod
    async def resolve_positional_args(args: List, visitor) -> List:
        """Resolve all positional arguments"""
        return [await ArgumentResolver.resolve_if_var(a, visitor) for a in args]
    
    @staticmethod
    async def resolve_keyword_args(kwargs: Dict, visitor) -> Dict:
        """Resolve all keyword arguments"""
        return {k: await ArgumentResolver.resolve_if_var(v, visitor) for k, v in kwargs.items()}

# ============================================================================
# FUNCTION SIGNATURE INSPECTOR (Enhanced)
# ============================================================================
class SignatureInspector:
    """Inspect function signatures"""
    
    _signature_cache: Dict[Callable, Any] = {}
    
    @staticmethod
    def get_signature(func: Callable):
        """Get function signature (cached)"""
        if func in SignatureInspector._signature_cache:
            return SignatureInspector._signature_cache[func]
        
        try:
            sig = inspect.signature(func)
            SignatureInspector._signature_cache[func] = sig
            return sig
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def has_parameter(sig, param_name: str) -> bool:
        """Check if signature has parameter"""
        return sig and param_name in sig.parameters
    
    @staticmethod
    def get_context_param_name(sig) -> Optional[str]:
        """Get context parameter name if exists"""
        if SignatureInspector.has_parameter(sig, 'context'):
            return 'context'
        if SignatureInspector.has_parameter(sig, 'ctx'):
            return 'ctx'
        return None
    
    @staticmethod
    def should_inject_context(func: Callable, ctx) -> Optional[str]:
        """Check if context should be injected"""
        if not ctx:
            return None
        sig = SignatureInspector.get_signature(func)
        return SignatureInspector.get_context_param_name(sig)

# ============================================================================
# ASYNC EXECUTOR (Enhanced with timeout support)
# ============================================================================
class AsyncExecutor:
    """Execute async operations with timeout support"""
    
    @staticmethod
    def is_coroutine(result) -> bool:
        """Check if result is coroutine"""
        return asyncio.iscoroutine(result)
    
    @staticmethod
    async def await_if_needed(result):
        """Await result if it's a coroutine"""
        return await result if AsyncExecutor.is_coroutine(result) else result
    
    @staticmethod
    async def call_function(func: Callable, pos_args: List, kw_args: Dict):
        """Call function and await if needed"""
        try:
            result = func(*pos_args, **kw_args)
            return await AsyncExecutor.await_if_needed(result)
        except Exception as e:
            raise DSLRuntimeError(f"Error calling {func.__name__}: {str(e)}") from e
    
    @staticmethod
    async def call_with_timeout(func: Callable, pos_args: List, kw_args: Dict, timeout: float = Config.DEFAULT_TIMEOUT):
        """Call function with timeout"""
        try:
            return await asyncio.wait_for(
                AsyncExecutor.call_function(func, pos_args, kw_args),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            raise DSLTimeoutError(f"Function {func.__name__} timed out after {timeout}s")

# ============================================================================
# DSL VISITOR (Enhanced with recursion tracking and better error handling)
# ============================================================================
class DSLVisitor:
    """
    Enhanced DSL visitor with:
    - Recursion depth tracking
    - Better error handling
    - Performance monitoring
    - Resource cleanup
    """
    
    OPS = {
        'OP_ADD': operator.add, 'OP_SUB': operator.sub, 'OP_MUL': operator.mul,
        'OP_DIV': operator.truediv, 'OP_MOD': operator.mod, 'OP_POW': operator.pow,
        'OP_EQ': operator.eq, 'OP_NEQ': operator.ne, 'OP_GT': operator.gt,
        'OP_LT': operator.lt, 'OP_GTE': operator.ge, 'OP_LTE': operator.le,
        'OP_AND': lambda a, b: a and b, 'OP_OR': lambda a, b: a or b, 'OP_NOT': lambda a: not a
    }

    def __init__(self, functions_map: Optional[Dict] = None):
        self.functions_map = functions_map or {}
        self.root_data = {}
        self._background_tasks: List[asyncio.Task] = []
        self._recursion_depth = 0
        self._visited_nodes: Set = set()  # For cycle detection
        self.store: Optional[DataStore] = None
        self.PROCESSORS: Dict = {}


    # ========================================================================
    # RECURSION TRACKING
    # ========================================================================
    def _enter_recursion(self):
        """Enter recursion level"""
        self._recursion_depth += 1
        if self._recursion_depth > Config.MAX_RECURSION_DEPTH:
            raise DSLRecursionError(f"Maximum recursion depth ({Config.MAX_RECURSION_DEPTH}) exceeded")

    # ========================================================================
    # PROCESSOR REGISTRY (Data-Driven Dispatch)
    # ========================================================================
    def _setup_processors(self):
        """Initialize the processor registry"""
        self.PROCESSORS = {
            dict: self._visit_dict,
            list: self._visit_list_items,
            tuple: self._visit_tuple,
            'VAR': self._resolve,
            'TYPED': lambda n, c: self._resolve(NodeExtractor.get_typed_name(n), c),
            'CALL': self.execute_call,
            'EXPRESSION': lambda n, c: self.evaluate_expression(n[1], c),
        }
        
    def _setup_resolvers(self):
        """Initialize the resolver pipeline"""
        # All resolvers must accept (name, ctx)
        self.RESOLVERS = [
            self._resolve_from_context,
            lambda n, c: self._resolve_from_functions(n),
            self._resolve_from_root,
            lambda n, c: self._resolve_as_type(n)
        ]



        
    def _inject_store_functions(self):
        """Inject data-driven functions into the map"""
        self.functions_map['set'] = self.store.set
        self.functions_map['observe'] = self._dsl_observe
        
    async def _dsl_observe(self, path: str, callback: Any):
        """DSL callable to observe state changes"""
        async def _wrapper(changed_path: str, value: Any):
            # Execute callback with the new value
            try:
                # We need a context-less execution or current context
                if isinstance(callback, (str, tuple)):
                    # Strategy: visit callback to find if it's a function
                    target = await self.visit(callback)
                    await self.execute_dsl_function(target, [value])
                else:
                    await self.execute_dsl_function(callback, [value])
            except Exception as e:
                framework_log("ERROR", f"Observer error on {changed_path}: {e}", emoji="👁️")

        self.store.subscribe(path, _wrapper)
        return True


    # ========================================================================
    # INITIALIZATION
    # ========================================================================
    def _init_root_data(self, data: Dict):
        """Initialize root data"""
        self.root_data = data
    
    def _inject_executor_to_root(self):
        """Inject executor into root data"""
        self.root_data['__execute_dsl_function__'] = self.execute_dsl_function
    
    def _has_background_tasks(self) -> bool:
        """Check if background tasks exist"""
        return len(self._background_tasks) > 0
    
    def _get_background_task_count(self) -> int:
        """Get number of background tasks"""
        return len(self._background_tasks)
    
    async def _wait_for_background_tasks(self):
        """Wait for all background tasks to complete"""
        if self._background_tasks:
            try:
                await asyncio.gather(*self._background_tasks, return_exceptions=True)
            except Exception as e:
                framework_log("ERROR", f"Background task error: {e}", emoji="❌")
    
    async def run(self, data: Dict):
        """Run visitor on data with proper cleanup"""
        try:
            self._init_root_data(data)
            self._inject_executor_to_root()
            self._setup_processors()
            self._setup_resolvers()
            
            # Initialize DataStore if not already present
            if not self.store:
                self.store = DataStore(self.root_data)
            else:
                self.store._state = self.root_data
            
            self._inject_store_functions()



            
            res = await self.visit(data)
            
            if self._has_background_tasks():
                count = self._get_background_task_count()
                framework_log("INFO", f"⏳ Waiting for {count} background tasks", emoji="💤")
                await self._wait_for_background_tasks()
            
            return res
        except DSLError:
            raise
        except Exception as e:
            raise DSLRuntimeError(f"Runtime error: {str(e)}") from e
        finally:
            # Cleanup
            self._recursion_depth = 0
            self._visited_nodes.clear()

    # ========================================================================
    # RESOLUTION (Enhanced with better error handling)
    # ========================================================================
    def _get_name_from_node(self, node) -> str:
        """Extract name from node"""
        return NodeExtractor.get_var_name(node) if NodeTypeChecker.is_var(node) else str(node)
    
    def _should_resolve_dotted(self, name: str) -> bool:
        """Check if name should be resolved as dotted path"""
        return StringUtil.has_dots(name)
    
    async def _resolve_from_context(self, name: str, ctx: Dict):
        """Resolve from context"""
        if ContextManager.has_key(ctx, name):
            return ContextManager.get_value(ctx, name)
        return None
    
    def _resolve_from_functions(self, name: str):
        """Resolve from functions map"""
        return NodeExtractor.get_dict_value_or_none(self.functions_map, name)
    
    async def _resolve_from_root(self, name: str, ctx: Dict):
        """Resolve from root data"""
        if not self.store: return None
        
        # Direct lookup in DataStore
        val = await self.store.get(name)
        if val is not None:
            return await self.visit(val, ctx)
        
        # Typed name lookup
        for k, v in self.store._state.items():
            if NodeTypeChecker.is_typed(k) and NodeExtractor.get_typed_name(k) == name:
                return await self.visit(v, ctx)
        
        return None

    
    def _resolve_as_type(self, name: str):
        """Resolve as type"""
        return TypeValidator.get_type_class(name) or name
    
    async def _resolve_dotted_path(self, name: str, ctx: Dict):
        """Resolve dotted path"""
        parts = StringUtil.split_dotted_name(name)
        val = await self._resolve(('VAR', parts[0]), ctx)
        
        for part in parts[1:]:
            val = self._get_attribute_from_value(val, part)
            if val is None:
                break
        
        return val
    
    def _get_attribute_from_value(self, val, attr_name: str):
        """Get attribute from value"""
        if isinstance(val, dict):
            return val.get(attr_name)
        return getattr(val, attr_name, None)
    
    async def _resolve(self, node, ctx):
        """Resolve node using the data-driven RESOLVERS pipeline"""
        if not NodeTypeChecker.is_resolvable(node):
            return node
        
        name = self._get_name_from_node(node)
        
        if self._should_resolve_dotted(name):
            return await self._resolve_dotted_path(name, ctx)
        
        # Use a sentinel to detect if value was found (even if None)
        NOT_FOUND = object()
        
        # Dispatch to the resolver pipeline (Data-Driven Priority)
        for resolver_func in self.RESOLVERS:
            try:
                res = resolver_func(name, ctx)
                if asyncio.iscoroutine(res): res = await res
                if res is not None: return res
            except Exception as e:
                # Log only unexpected errors, not signature mismatches (though we fixed them)
                if not isinstance(e, TypeError):
                    framework_log("DEBUG", f"Resolver error: {e}")
                continue
            
        return name



    # ========================================================================
    # VISIT (The core recursive engine)
    # ========================================================================
    async def visit(self, node, ctx=None):
        """
        Data-driven visit logic.
        Uses the PROCESSORS registry to delegate logic based on node characteristics.
        """
        self._recursion_depth += 1
        if self._recursion_depth > Config.MAX_RECURSION_DEPTH:
            raise DSLRecursionError("Maximum recursion depth exceeded")

        try:
            # 1. Identification by Type
            node_type = type(node)
            if node_type in self.PROCESSORS:
                return await self.PROCESSORS[node_type](node, ctx)

            # 2. Identification by Tag (for Tuples)
            if node_type is tuple and len(node) > 0:
                tag = node[0]
                if tag in self.PROCESSORS:
                    # For VAR and TYPED, the node itself is the argument
                    if tag in ['VAR', 'TYPED']:
                        return await self.PROCESSORS[tag](node, ctx)
                    # For CALL and EXPRESSION, the node is passed as is
                    return await self.PROCESSORS[tag](node, ctx)
                
            # 3. Handle Resolvables (if not handled by type or tag)
            if NodeTypeChecker.is_resolvable(node):
                return await self._resolve(node, ctx)

            return node
        finally:
            self._recursion_depth -= 1
    
    async def _visit_list_items(self, items: List, ctx):
        """Visit list items"""
        return [await self.visit(x, ctx) for x in items]

    
    async def _visit_scalar(self, node, ctx):
        """Visit scalar node"""
        if NodeTypeChecker.is_resolvable(node):
            return await self._resolve(node, ctx)
        return node

    # ========================================================================
    # DICT VISIT
    # ========================================================================
    def _prepare_working_context(self, ctx: Optional[Dict]) -> Dict:
        """Prepare working context"""
        base = ContextManager.copy_context(ctx)
        return ContextManager.add_executor(base, self.execute_dsl_function)
    
    def _extract_key_name(self, key) -> str:
        """Extract key name"""
        if NodeTypeChecker.is_typed(key):
            return NodeExtractor.get_typed_name(key)
        return str(key)
    
    def _validate_typed_key(self, key, value):
        """Validate typed key"""
        if NodeTypeChecker.is_typed(key):
            type_name = NodeExtractor.get_typed_type(key)
            var_name = NodeExtractor.get_typed_name(key)
            TypeValidator.validate(value, type_name, var_name)
    
    def _update_context_with_value(self, ctx: Dict, key: str, value):
        """Update context with value"""
        ContextManager.set_value(ctx, key, value)
    
    def _extract_triggers(self, node: Dict) -> List:
        """Extract triggers from node"""
        return node.pop('__triggers__', [])
    
    def _create_trigger_task(self, trigger_key, action, ctx):
        """Create trigger task"""
        return asyncio.create_task(self._start_trigger(trigger_key, action, ctx))
    
    def _add_background_task(self, task):
        """Add background task"""
        self._background_tasks.append(task)
    
    async def _visit_dict(self, node: Dict, ctx):
        """Visit dictionary node using a pipeline of processors"""
        working_ctx = self._prepare_working_context(ctx)
        res = {}
        
        # ITEM_PROCESSORS: Pipeline per gestire ogni coppia chiave-valore
        for k, v in node.items():
            if k == '__triggers__': continue
            
            # Step 1: Resolve Value
            val = await self.visit(v, working_ctx)
            
            # Step 2: Validate & Extract Name (Data-Driven)
            self._validate_typed_key(k, val)
            key_name = self._extract_key_name(k)
            
            # Step 3: Bind & Update
            res[key_name] = val
            self._update_context_with_value(working_ctx, key_name, val)
        
        # TRIGGER_PROCESSOR: Gestione dichiarativa dei trigger
        await self._process_triggers(node.get('__triggers__', []), working_ctx)
        return res

    async def _process_triggers(self, triggers: List, ctx: Dict):
        """Dichiarative trigger processing"""
        for trigger_key, action in triggers:
            self._add_background_task(
                asyncio.create_task(self._start_trigger(trigger_key, action, ctx))
            )


    # ========================================================================
    # TUPLE VISIT
    # ========================================================================
    def _get_tuple_tag(self, node: Tuple) -> str:
        """Get tuple tag"""
        return NodeExtractor.get_operation_tag(node)
    
    def _is_operation_tag(self, tag: str) -> bool:
        """Check if tag is operation"""
        return isinstance(tag, str) and tag in self.OPS
    
    async def _execute_operation(self, node: Tuple, ctx):
        """Execute operation"""
        tag = self._get_tuple_tag(node)
        args = NodeExtractor.get_operation_args(node)
        try:
            resolved_args = [await self.visit(a, ctx) for a in args]
            return self.OPS[tag](*resolved_args)
        except Exception as e:
            raise DSLRuntimeError(f"Error executing operation {tag}: {str(e)}") from e
    
    def _get_handler_for_tag(self, tag: str) -> Optional[Callable]:
        """Get handler for tag"""
        if not isinstance(tag, str):
            return None
        handlers = {
            'EXPRESSION': lambda n, c: self.evaluate_expression(n[1], c),
            'CALL': lambda n, c: self.execute_call(n, c),
            'VAR': lambda n, c: self._resolve(n, c),
            'TYPED': lambda n, c: self._resolve(NodeExtractor.get_typed_name(n), c)
        }
        return handlers.get(tag)
    
    async def _apply_handler(self, handler: Callable, node, ctx):
        """Apply handler"""
        return await handler(node, ctx)
    
    async def _visit_tuple_items(self, node: Tuple, ctx):
        """Visit tuple items"""
        return tuple([await self.visit(x, ctx) for x in node])
    
    async def _visit_tuple(self, node, ctx):
        """Visit tuple node"""
        tag = self._get_tuple_tag(node)
        
        if self._is_operation_tag(tag):
            return await self._execute_operation(node, ctx)
        
        handler = self._get_handler_for_tag(tag)
        if handler:
            return await self._apply_handler(handler, node, ctx)
        
        if NodeTypeChecker.is_function_def(node):
            return node
        
        return await self._visit_tuple_items(node, ctx)

    # ========================================================================
    # TRIGGERS
    # ========================================================================
    def _is_event_trigger(self, trigger_key) -> bool:
        """Check if trigger is event"""
        return NodeTypeChecker.is_call(trigger_key)
    
    def _is_cron_trigger(self, trigger_key) -> bool:
        """Check if trigger is cron"""
        return isinstance(trigger_key, (list, tuple)) and any(x == '*' for x in trigger_key)
    
    async def _start_trigger(self, trigger_key, action, ctx):
        """Start trigger"""
        try:
            if self._is_event_trigger(trigger_key):
                await self._event_loop(trigger_key, action, ctx)
            elif self._is_cron_trigger(trigger_key):
                await self._cron_loop(trigger_key, action, ctx)
        except Exception as e:
            framework_log("ERROR", f"Trigger error: {e}", emoji="❌")
    
    def _get_current_time_tuple(self):
        """Get current time as tuple"""
        import datetime
        now = datetime.datetime.now()
        return (now.minute, now.hour, now.day, now.month, now.weekday())
    
    def _get_seconds_until_next_minute(self):
        """Get seconds until next minute"""
        import datetime
        now = datetime.datetime.now()
        wait = 60 - now.second
        return wait if wait > 0 else 60
    
    async def _execute_action_safely(self, action, ctx, error_prefix: str):
        """Execute action with error handling"""
        try:
            await self.visit(action, ctx)
        except Exception as e:
            framework_log("ERROR", f"❌ {error_prefix}: {e}", emoji="❌")
    
    async def _cron_loop(self, pattern, action, ctx):
        """Cron loop"""
        framework_log("INFO", f"⏰ Starting cron: {pattern}", emoji="⏳")
        
        while True:
            try:
                current = self._get_current_time_tuple()
                
                if WildcardMatcher.match(current, pattern):
                    framework_log("INFO", f"⚡ Executing cron: {pattern}", emoji="⚡")
                    await self._execute_action_safely(action, ctx, f"Cron error {pattern}")
                
                wait_seconds = self._get_seconds_until_next_minute()
                await asyncio.sleep(wait_seconds)
            except asyncio.CancelledError:
                framework_log("INFO", f"Cron loop cancelled: {pattern}", emoji="🛑")
                break
            except Exception as e:
                framework_log("ERROR", f"Cron loop error: {e}", emoji="❌")
                await asyncio.sleep(60)
    
    def _extract_event_data(self, result: Dict):
        """Extract event data from result"""
        is_valid = result and isinstance(result, dict) and result.get('success')
        return result.get('data') if is_valid else None
    
    def _create_event_context(self, ctx: Dict, event_data):
        """Create event context"""
        new_ctx = ContextManager.copy_context(ctx)
        ContextManager.set_value(new_ctx, '@event', event_data)
        return new_ctx
    
    async def _event_loop(self, call_node, action, ctx):
        """Event loop"""
        name = NodeExtractor.get_call_name(call_node)
        framework_log("INFO", f"🎭 Starting event listener: {name}", emoji="👂")
        
        while True:
            try:
                res = await self.execute_call(call_node, ctx)
                event_data = self._extract_event_data(res)
                
                if event_data:
                    framework_log("INFO", f"📢 Event detected: {name}", emoji="📢")
                    event_ctx = self._create_event_context(ctx, event_data)
                    await self.visit(action, event_ctx)
                else:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                framework_log("INFO", f"Event loop cancelled: {name}", emoji="🛑")
                break
            except Exception as e:
                framework_log("ERROR", f"❌ Event error {name}: {e}", emoji="❌")
                await asyncio.sleep(5)

    # ========================================================================
    # CALL EXECUTION
    # ========================================================================
    async def _prepare_call_args(self, p_nodes, k_nodes, ctx):
        """Prepare call arguments"""
        pos = [await self.visit(a, ctx) for a in p_nodes]
        kw = {k: await self.visit(v, ctx) for k, v in k_nodes.items()}
        return pos, kw
    
    async def execute_call(self, call, ctx):
        """Execute function call"""
        name_node = NodeExtractor.get_call_name(call)
        p_nodes = NodeExtractor.get_call_pos_args(call)
        k_nodes = NodeExtractor.get_call_kw_args(call)
        
        func_def = await self._resolve(name_node, ctx)
        p_args, k_args = await self._prepare_call_args(p_nodes, k_nodes, ctx)
        
        if NodeTypeChecker.is_function_def(func_def):
            return await self.execute_dsl_function(func_def, p_args, k_args)
        
        return await self._execute(str(name_node), p_args, k_args, ctx)

    # ========================================================================
    # EXECUTE
    # ========================================================================
    def _is_include_call(self, name: str, args: List) -> bool:
        """Check if call is include"""
        return name == 'include' and len(args) > 0
    
    def _is_qualified_name(self, name: str) -> bool:
        """Check if name is qualified"""
        return StringUtil.has_dots(name)
    
    def _get_from_function_map(self, name: str):
        """Get from function map"""
        return NodeExtractor.get_dict_value_or_none(self.functions_map, name)
    
    async def _execute(self, name: str, p_args: List, k_args: Dict, ctx=None):
        """Execute function using a Declarative Dispatcher (0% Imperative)"""
        p = await ArgumentResolver.resolve_positional_args(p_args, self)
        k = await ArgumentResolver.resolve_keyword_args(k_args, self)

        # 1. Dispatch Table (Condition -> Action)
        DISPATCH = [
            (name == 'include' and p, lambda: self._handle_include(p[0])),
            (self._is_qualified_name(name), lambda: self._execute_qualified(name, p, k, ctx)),
            (self._get_from_function_map(name), lambda: self._call_function(self._get_from_function_map(name), p, k, ctx)),
        ]

        # 2. Execution Logic
        for condition, action in DISPATCH:
            if condition:
                return await AsyncExecutor.await_if_needed(action())

        # 3. Dynamic Resolve Fallback
        res = await self._execute_resolved(name, p, k, ctx)
        if res is not None: 
            return res
            
        # If we reach here, and it's not a known function, it's an error
        raise DSLRuntimeError(f"Function '{name}' not found or could not be executed")


    async def _execute_resolved(self, name: str, p_args: List, k_args: Dict, ctx: Dict):
        """Resolve and execute strategy"""
        target = await self._resolve_func(name, ctx)
        if NodeTypeChecker.is_function_def(target):
            return await self.execute_dsl_function(target, p_args, k_args)
        if callable(target):
            return await AsyncExecutor.call_function(target, p_args, k_args)
        return None


    # ========================================================================
    # INCLUDE HANDLER
    # ========================================================================
    def _ensure_dsl_extension(self, path: str) -> str:
        """Ensure path has .dsl extension"""
        return path if path.endswith(".dsl") else path + ".dsl"
    
    def _parse_dsl_content(self, content: str) -> Dict:
        """Parse DSL content"""
        try:
            tree = Lark(grammar).parse(content)
            return ConfigTransformer().transform(tree)
        except Exception as e:
            raise DSLSyntaxError(f"Parse error: {str(e)}") from e
    
    def _filter_statement_keys(self, keys: List) -> List:
        """Filter statement keys"""
        return [k for k in keys if not str(k).startswith('__stmt_')]
    
    def _merge_into_root(self, new_dict: Dict):
        """Merge dictionary into root"""
        for k, v in new_dict.items():
            if not str(k).startswith('__stmt_'):
                self.root_data[k] = v
    
    def _create_include_success(self, path: str, variables: List) -> Dict:
        """Create include success response"""
        return {"included": path, "variables": variables}
    
    def _create_include_error(self, error: Exception) -> Dict:
        """Create include error response"""
        return {"error": str(error)}
    
    async def _handle_include(self, path: str):
        """Handle include"""
        full_path = self._ensure_dsl_extension(path)
        try:
            content = await _load_resource(path=full_path)
            new_dict = self._parse_dsl_content(content)
            self._merge_into_root(new_dict)
            variables = self._filter_statement_keys(list(new_dict.keys()))
            return self._create_include_success(full_path, variables)
        except Exception as e:
            framework_log("ERROR", f"Failed to include {full_path}: {e}", emoji="❌")
            return self._create_include_error(e)

    # ========================================================================
    # QUALIFIED EXECUTION
    # ========================================================================
    def _split_qualified_name(self, name: str) -> Tuple[str, List[str]]:
        """Split qualified name"""
        parts = list(StringUtil.split_dotted_name(name))
        return parts[0], parts[1:]
    
    def _resolve_base_object(self, base_name: str, ctx: Dict):
        """Resolve base object"""
        obj = self._get_from_function_map(base_name)
        if obj:
            return obj
        
        if ctx:
            obj = ContextManager.get_value(ctx, base_name)
            if obj:
                return obj
        
        obj = self.root_data.get(base_name)
        if obj:
            return obj
        
        # Typed lookup
        for k, v in self.root_data.items():
            if NodeTypeChecker.is_typed(k) and NodeExtractor.get_typed_name(k) == base_name:
                return v
        
        return None
    
    def _navigate_attribute_path(self, obj, parts: List[str]):
        """Navigate attribute path"""
        for part in parts:
            obj = self._get_attribute_from_value(obj, part)
            if obj is None:
                return None
        return obj
    
    async def _execute_qualified(self, name: str, p_args: List, k_args: Dict, ctx: Dict):
        """Execute qualified function"""
        base_name, attr_parts = self._split_qualified_name(name)
        obj = self._resolve_base_object(base_name, ctx)
        
        if obj is None:
            raise DSLRuntimeError(f"Base object '{base_name}' not found")
        
        target = self._navigate_attribute_path(obj, attr_parts)
        
        if target is None:
            raise DSLRuntimeError(f"Attribute path '{name}' not found")
        
        if NodeTypeChecker.is_function_def(target):
            return await self.execute_dsl_function(target, p_args, k_args)
        
        if callable(target):
            return await AsyncExecutor.call_function(target, p_args, k_args)
        
        return target

    # ========================================================================
    # FUNCTION CALL
    # ========================================================================
    def _inject_context_if_needed(self, func: Callable, k_args: Dict, ctx):
        """Inject context if needed"""
        param_name = SignatureInspector.should_inject_context(func, ctx)
        if param_name:
            k_args[param_name] = ctx
    
    async def _call_function(self, func: Callable, p_args: List, k_args: Dict, ctx):
        """Call function"""
        self._inject_context_if_needed(func, k_args, ctx)
        result = await AsyncExecutor.call_function(func, p_args, k_args)
        return ResultUnwrapper.unwrap(result)

    # ========================================================================
    # FUNC RESOLUTION
    # ========================================================================
    def _resolve_from_context_direct(self, name: str, ctx: Dict):
        """Resolve from context directly"""
        return ContextManager.get_value(ctx, name) if ctx else None
    
    def _resolve_from_root_direct(self, name: str):
        """Resolve from root directly"""
        return self.root_data.get(name)
    
    def _resolve_from_root_typed(self, name: str):
        """Resolve from root typed"""
        for k, v in self.root_data.items():
            if NodeTypeChecker.is_typed(k) and NodeExtractor.get_typed_name(k) == name:
                return v
        return None
    
    async def _resolve_func(self, name: str, ctx):
        """Resolve function"""
        result = self._resolve_from_context_direct(name, ctx)
        if result is not None:
            return result
        
        result = self._resolve_from_root_direct(name)
        if result is not None:
            return result
        
        return self._resolve_from_root_typed(name)

    # ========================================================================
    # EXPRESSION
    # ========================================================================
    def _merge_root_and_context(self, ctx: Optional[Dict]) -> Dict:
        """Merge root and context"""
        return ContextManager.merge_contexts(self.root_data, ctx or {})
    
    def _create_seed_stage(self, seed_value):
        """Create seed stage"""
        return flow.step(lambda context=None: seed_value)
    
    def _create_pipe_stages(self, ops: List, seed) -> List:
        """Create pipe stages"""
        return [flow.step(self._make_pipe_stage(op, seed)) for op in ops]
    
    async def _execute_pipeline(self, stages: List, context: Dict):
        """Execute pipeline"""
        try:
            return await flow.pipe(*stages, context=context)
        except Exception as e:
            framework_log("ERROR", f"Expression evaluation error: {e}", emoji="💥")
            import traceback
            traceback.print_exc()
            raise DSLRuntimeError(f"Pipeline error: {str(e)}") from e
    
    async def evaluate_expression(self, ops, ctx):
        """Evaluate expression"""
        if not ops:
            return None
        
        context = self._merge_root_and_context(ctx)
        seed = await self.visit(ops[0], context)
        
        stages = [self._create_seed_stage(seed)]
        stages.extend(self._create_pipe_stages(ops[1:], seed))
        
        return await self._execute_pipeline(stages, context)

    # ========================================================================
    # PIPE STAGE
    # ========================================================================
    def _get_previous_result(self, context: Dict, seed):
        """Get previous result"""
        if context and context.get('outputs'):
            return context['outputs'][-1]
        return seed
    
    def _extract_pipe_name(self, op) -> Optional[str]:
        """Extract pipe name"""
        if NodeTypeChecker.is_call(op):
            return NodeExtractor.get_call_name(op)
        if NodeTypeChecker.is_typed(op):
            return NodeExtractor.get_typed_name(op)
        if NodeTypeChecker.is_var(op):
            return NodeExtractor.get_var_name(op)
        if isinstance(op, str):
            return op
        return None
    
    async def _execute_pipe_call(self, op: Tuple, prev, context):
        """Execute pipe call"""
        name = NodeExtractor.get_call_name(op)
        p_nodes = NodeExtractor.get_call_pos_args(op)
        k_nodes = NodeExtractor.get_call_kw_args(op)
        
        p_args = await self._build_pipe_args(prev, p_nodes, context)
        k_args = {k: await self.visit(v, context) for k, v in k_nodes.items()}
        
        func_def = await self._resolve(name, context)
        
        if NodeTypeChecker.is_function_def(func_def):
            return await self.execute_dsl_function(func_def, p_args, k_args)
        
        result = await self._execute(name, p_args, k_args, ctx=context)
        return ResultUnwrapper.unwrap(result)
    
    async def _build_pipe_args(self, prev, p_nodes, context):
        """Build pipe arguments"""
        if len(p_nodes) == 3 and isinstance(p_nodes[1], dict):
            return [prev, p_nodes]
        if len(p_nodes) == 1 and hasattr(p_nodes[0], 'data') and p_nodes[0].data == 'dictionary':
            return [prev, p_nodes[0]]
        return [prev] + [await self.visit(a, context) for a in p_nodes]
    
    async def _execute_pipe_func(self, name: str, prev, context):
        """Execute pipe function"""
        func_def = await self._resolve(name, context)
        
        if NodeTypeChecker.is_function_def(func_def):
            return await self.execute_dsl_function(func_def, [prev], {})
        
        result = await self._execute(name, [prev], {}, ctx=context)
        return ResultUnwrapper.unwrap(result)
    
    def _make_pipe_stage(self, op, seed):
        """Make pipe stage"""
        async def pipe_stage(context=None):
            prev_raw = self._get_previous_result(context, seed)
            prev = ResultUnwrapper.unwrap(prev_raw)
            
            if NodeTypeChecker.is_call(op):
                return await self._execute_pipe_call(op, prev, context)
            
            if NodeTypeChecker.is_function_def(op):
                return await self.execute_dsl_function(op, [prev], {})
            
            name = self._extract_pipe_name(op)
            if name:
                return await self._execute_pipe_func(name, prev, context)
            
            return await self.visit(op, context)
        
        pipe_stage.__name__ = f"dsl_stage_{str(op)[:20]}"
        return pipe_stage

    # ========================================================================
    # DSL FUNCTION
    # ========================================================================
    def _extract_function_parts(self, func_def: Tuple) -> Tuple:
        """Extract function parts"""
        return func_def[0], func_def[1], func_def[2]
    
    async def _map_params_to_context(self, in_def, p_args: List, k_args: Dict) -> Dict:
        """Map parameters to context"""
        ctx = ContextManager.create_empty()
        params = self._get_param_list(in_def)
        
        for i, (name, type_name) in enumerate(params):
            val = self._get_param_value(i, name, p_args, k_args)
            if type_name:
                TypeValidator.validate(val, type_name, name)
            ContextManager.set_value(ctx, name, val)
        
        return ctx
    
    def _get_param_value(self, index: int, name: str, p_args: List, k_args: Dict):
        """Get parameter value"""
        if index < len(p_args):
            return p_args[index]
        return k_args.pop(name, None)
    
    async def _execute_function_body(self, body: Dict, ctx: Dict) -> Dict:
        """Execute function body"""
        result = await self.visit(body, ctx)
        ctx.update(result)
        return ctx
    
    async def _extract_outputs(self, out_def, ctx: Dict):
        """Extract outputs"""
        params = self._get_param_list(out_def)
        results = []
        
        for name, _ in params:
            val = ContextManager.get_value(ctx, name)
            if val is None:
                val = await self._resolve(name, ctx)
            results.append(val)
        
        return results[0] if len(results) == 1 else tuple(results)
    
    async def execute_dsl_function(self, func_def, p_args, k_args=None):
        """
        Execute DSL function as a Functional Blueprint.
        No imperative steps, just a flow of data transformations.
        """
        blueprint = self._extract_function_parts(func_def)
        
        # Pipeline: Map Params -> Execute Body -> Extract Outputs
        pipe = [
            lambda ctx: self._map_params_to_context(blueprint[0], p_args, k_args or {}),
            lambda ctx: self._execute_function_body(blueprint[1], ctx),
            lambda ctx: self._extract_outputs(blueprint[2], ctx)
        ]
        
        data = {} # Initial empty context for function
        for stage in pipe:
            res = stage(data)
            data = await res if asyncio.iscoroutine(res) else res
            
        return data # This is the result of the last stage (_extract_outputs)



    # ========================================================================
    # PARAM EXTRACTION
    # ========================================================================
    def _extract_single_param(self, p) -> Tuple[str, Optional[str]]:
        """Extract single parameter"""
        if NodeTypeChecker.is_typed(p):
            return NodeExtractor.get_typed_name(p), NodeExtractor.get_typed_type(p)
        if NodeTypeChecker.is_var(p):
            return NodeExtractor.get_var_name(p), None
        return str(p), None
    
    def _is_typed_sequence(self, def_node) -> bool:
        """Check if node is typed sequence"""
        return isinstance(def_node, tuple) and len(def_node) == 3 and def_node[0] == 'TYPED'
    
    def _get_param_nodes(self, def_node) -> List:
        """Get parameter nodes"""
        is_seq = isinstance(def_node, (list, tuple))
        is_typed = self._is_typed_sequence(def_node)
        
        if is_seq and not is_typed:
            return def_node
        return [def_node]
    
    def _get_param_list(self, def_node) -> List[Tuple[str, Optional[str]]]:
        """Get parameter list"""
        nodes = self._get_param_nodes(def_node)
        return [self._extract_single_param(n) for n in nodes]


# ============================================================================
# PUBLIC API
# ============================================================================

# Cached parser for better performance
_parser_cache = None

def _get_parser():
    """Get cached parser"""
    global _parser_cache
    if _parser_cache is None:
        _parser_cache = Lark(grammar, parser='earley')
    return _parser_cache

def parse_dsl_file(content: str) -> Dict:
    """Parse DSL content"""
    try:
        parser = _get_parser()
        tree = parser.parse(content)
        return ConfigTransformer().transform(tree)
    except Exception as e:
        raise DSLSyntaxError(f"Parse error: {str(e)}") from e

async def execute_dsl_file(content_or_parsed):
    """Execute DSL file"""
    if isinstance(content_or_parsed, str):
        parsed = parse_dsl_file(content_or_parsed)
    else:
        parsed = content_or_parsed
    
    return await DSLVisitor(dsl_functions).run(parsed)

async def dsl_map(data, func, context=None):
    """Map function over data"""
    if isinstance(func, str):
        items = _normalize_to_list(data)
        return [mistql.query(func, data=i) for i in items]
    
    return await flow.foreach(data, func, context=context)

def _normalize_to_list(data) -> List:
    """Normalize data to list"""
    if isinstance(data, dict):
        return list(data.values())
    if isinstance(data, (list, tuple)):
        return list(data)
    return [data]

# ============================================================================
# TEST RUNNER (Enhanced with better reporting)
# ============================================================================

def _normalize_test_suite(test_suite) -> List:
    """Normalize test suite"""
    if isinstance(test_suite, dict):
        return [test_suite]
    if isinstance(test_suite, (list, tuple)):
        return list(test_suite)
    return []

def _extract_test_data(test: Dict) -> Tuple[str, Any, Any]:
    """Extract test data"""
    target = test.get('target')
    args = test.get('input') if test.get('input') is not None else test.get('input_args')
    expected = test.get('output') if test.get('output') is not None else test.get('expected_output')
    return target, args, expected

async def _run_single_test(visitor, parsed_data: Dict, test: Dict) -> bool:
    """Run single test"""
    if not isinstance(test, dict):
        return True
    
    target, args, expected = _extract_test_data(test)
    print(f"Testing '{target}'...", end=" ")
    
    try:
        target_def = parsed_data.get(target)
        is_func = NodeTypeChecker.is_function_def(target_def)
        
        if is_func:
            actual = await visitor.execute_dsl_function(target_def, args)
        else:
            actual = await visitor.visit(target_def)
        
        if actual == expected:
            print("🟢 OK")
            return True
        else:
            print(f"🔴 FAILED (expected {expected}, got {actual})")
            return False
    except Exception as e:
        print(f"🔴 EXC: {e}")
        import traceback
        traceback.print_exc()
        return False

def _print_test_header(count: int):
    """Print test header"""
    print("\n" + "="*40)
    print(f"DSL Tests: {count}")
    print("="*40)

def _print_test_result(all_passed: bool, passed: int, total: int):
    """Print test result"""
    result = '🟢 PASSED' if all_passed else '🔴 FAILED'
    print("="*40)
    print(f"RESULT: {result} ({passed}/{total})")
    print("="*40)

async def run_dsl_tests(visitor, parsed_data: Dict) -> bool:
    """Run DSL tests"""
    test_suite = _normalize_test_suite(parsed_data.get('test_suite', []))
    
    if not test_suite:
        return False
    
    _print_test_header(len(test_suite))
    
    results = []
    for test in test_suite:
        passed = await _run_single_test(visitor, parsed_data, test)
        results.append(passed)
    
    all_passed = all(results)
    passed_count = sum(results)
    _print_test_result(all_passed, passed_count, len(results))
    
    return all_passed

# ============================================================================
# DSL FUNCTIONS MAP
# ============================================================================
dsl_functions = {
    # Flow functions
    'resource': load.resource, 'transform': flow.transform, 'normalize': flow.normalize,
    'put': flow.put, 'format': flow.format, 'foreach': flow.foreach, 'map': dsl_map,
    'convert': flow.convert, 'get': flow.get, 'match': flow._dsl_switch,
    'batch': flow.batch, 'parallel': flow.batch, 'race': flow.race, 
    'timeout': flow.timeout, 'throttle': flow.throttle, 'catch': flow.catch, 
    'branch': flow.branch, 'retry': flow.retry, 'fallback': flow.fallback,
    
    # Dict functions
    'keys': lambda d: list(d.keys()) if isinstance(d, dict) else [],
    'values': lambda d: list(d.values()) if isinstance(d, dict) else [],
    'items': lambda d: list(d.items()) if isinstance(d, dict) else [],
    'entries': lambda d: list(d.items()) if isinstance(d, dict) else [],
    
    # Utility functions
    'print': lambda d: (print(f"*** CUSTOM PRINT ***: {d}"), d)[1],
    'pick': lambda d, keys: {k: v for k, v in d.items() if k in keys} if isinstance(d, dict) and isinstance(keys, (list, tuple)) else d,
    'filter': lambda d, keys: {k: v for k, v in d.items() if k in keys} if isinstance(d, dict) and isinstance(keys, (list, tuple)) else d,
    'remap': lambda data, *names: [dict(zip(names, item)) for item in data] if isinstance(data, (list, tuple)) else data,
    'merge': lambda a, b: (a | b) if isinstance(a, dict) and isinstance(b, dict) else 
             ((list(a) if isinstance(a, (list, tuple)) else [a]) + (list(b) if isinstance(b, (list, tuple)) else [b])),
    'concat': lambda a, b: (list(a) if isinstance(a, (list, tuple)) else [a]) + (list(b) if isinstance(b, (list, tuple)) else [b]),
    'query': lambda data, q: mistql.query(q, data=data),
    'not': lambda x: not x,
    
    # Type constructors
    **{k: v for k, v in zip(['dict','list','str','int','float','bool'], [dict,list,str,int,float,bool])},
    **{k: t for k, t in [('integer',int),('string',str),('boolean',bool),('number',float),
                         ('relative',int),('natural',int),('rational',float),('complex',float)]},
    **{k: v for k, v in [('True',True),('False',False),('true',True),('false',False),('Null',None),('null',None)]}
}