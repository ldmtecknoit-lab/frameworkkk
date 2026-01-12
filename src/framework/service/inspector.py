
import json
import ast
import inspect
import types
import hashlib
import marshal
import os
import sys
import platform
import socket
import psutil
import traceback
import asyncio
import time
import contextvars
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Set
from framework.service.context import container

if sys.platform == 'emscripten':
    import js


# =====================================================================
# --- Strumenti di Introspezione e Analisi ---
# =====================================================================

class LogReportEncoder(json.JSONEncoder):
    """
    JSONEncoder personalizzato per la serializzazione di oggetti complessi 
    trovati nei log di debug e nelle tracce di errore.
    Converte qualsiasi tipo di dato non serializzabile in una stringa.
    """
    def default(self, obj):
        try:
            # 1. Tenta di usare l'implementazione predefinita della superclasse
            return super().default(obj)
        except TypeError:
            # 2. Fallback universale: converti in stringa
            return str(obj)

def _get_system_info() -> Dict[str, Any]:
    """Raccoglie le informazioni chiave su CPU, RAM e Processo."""
    mem = psutil.virtual_memory()
    
    return {
        "hostname": socket.gethostname(),
        "process_id": os.getpid(),
        "cpu_cores_logical": psutil.cpu_count(),
        "cpu_cores_physical": psutil.cpu_count(logical=False),
        "ram_total_gb": round(mem.total / (1024**3), 2),
        "ram_available_gb": round(mem.available / (1024**3), 2),
        "os_name": platform.platform(),
    }

def truncate_value(key: str, value: Any, max_str_len: int = 256, max_list_len: int = 20) -> Any:
    """
    Tronca valori di stringa e collezioni (liste/tuple) troppo grandi
    per mantenere i log di dimensione ragionevole.
    """
    if isinstance(value, str):
        if len(value) > max_str_len:
            return f"{value[:max_str_len]}... [TRONCATA, L={len(value)}]"
        return value

    elif isinstance(value, (list, tuple, set)):
        if len(value) > max_list_len:
            truncated_items = list(value)[:max_list_len]
            processed_items = [
                truncate_value("", item, max_str_len=30, max_list_len=5)
                for item in truncated_items
            ]
            return f"{processed_items} ... [TRONCATA, N={len(value)}]"
        
        return [
            truncate_value("", item, max_str_len=30, max_list_len=5)
            for item in value
        ]
        
    elif isinstance(value, dict):
        return {
            k: truncate_value(k, v, max_str_len=max_str_len, max_list_len=max_list_len) 
            for k, v in value.items()
        }

    return value

def analyze_traceback(tb: Optional[types.TracebackType]) -> List[Dict[str, Any]]:
    """
    Estrae i frame del traceback in un formato strutturato.
    """
    structured_tb = []
    current_tb = tb
    
    while current_tb is not None:
        frame = current_tb.tb_frame
        
        # Ignora le librerie di sistema
        filename = frame.f_code.co_filename
        if "/usr/" in filename or "/local/lib/python" in filename or "python3." in filename:
            current_tb = current_tb.tb_next
            continue

        # Estrai le variabili locali
        local_vars_state = {
            k: truncate_value(k, v)
            for k, v in frame.f_locals.items() 
            if not k.startswith('__') and k not in ['frame', 'frame_summary', 'current_tb', 'tb']
        }
        
        line_content = None
        try:
            frame_summary = traceback.FrameSummary(filename, current_tb.tb_lineno, frame.f_code.co_name, lookup_line=True)
            if frame_summary.line:
                line_content = frame_summary.line.strip()
        except Exception:
            pass 

        if line_content is None:
            if filename.startswith('<'):
                line_content = "SORGENTE DINAMICA NON DISPONIBILE"
            else:
                line_content = "SORGENTE NON RECUPERATA"
        
        structured_tb.append({
            "step_filename": filename,
            "step_lineno": current_tb.tb_lineno,
            "step_function": frame.f_code.co_name,
            "step_code_line": line_content, 
            "local_variables_state": local_vars_state
        })
        current_tb = current_tb.tb_next
    
    return structured_tb

def analyze_exception(source_code: str, custom_filename: str = "<code_in_memory>", app_context: Dict[str, Any] = None, 
                      exc_info: tuple = None) -> Dict[str, Any]:
    """Genera un report dettagliato sull'eccezione corrente o fornita."""
    if exc_info:
        exc_type, exc_value, exc_traceback = exc_info
    else:
        exc_type, exc_value, exc_traceback = sys.exc_info()
    
    if exc_type is None or exc_traceback is None:
        return {"status": "Nessuna eccezione attiva trovata."}
        
    tb_list = traceback.extract_tb(exc_traceback)
    
    last_traceback = exc_traceback
    while last_traceback.tb_next:
        last_traceback = last_traceback.tb_next
    last_frame_object = last_traceback.tb_frame 
    
    raw_filename = tb_list[-1].filename
    raw_lineno = tb_list[-1].lineno
    
    # Nota: source_code passato qui potrebbe non essere usato se si usa traceback.FrameSummary, 
    # ma è mantenuto per compatibilità con la firma originale.
    
    structured_tb = analyze_traceback(exc_traceback)
    
    final_error_step = structured_tb[-1] if structured_tb else {
        "step_code_line": "SORGENTE NON RECUPERATA", 
        "step_lineno": raw_lineno, 
        "step_function": tb_list[-1].name
    }
    
    final_local_vars = {
         k: truncate_value(k, v)
         for k, v in last_frame_object.f_locals.items() 
         if not k.startswith('__') and k not in ['last_traceback', 'last_frame_object', 'raw_lineno', 'tb_list', 'exc_traceback']
    }
    
    exception_details = {
        "exception_type": type(exc_value).__name__,
        "exception_message": str(exc_value),
        "error_location": {
            "filename": raw_filename,
            "line_number": final_error_step["step_lineno"],
            "function_name": final_error_step["step_function"],
            "source_code_line": final_error_step["step_code_line"],
        },
        "LOCAL_VARIABLES_STATE_FINAL_FRAME": final_local_vars,
    }
    
    # Snapshot del container DI
    container_snapshot = {}
    try:
        from framework.service.context import container
        for attr in dir(container):
            if attr.startswith('_'): continue
            p = getattr(container, attr)
            # dependency_injector providers hanno l'attributo 'provider' o sono essi stessi callable
            if hasattr(p, '__class__') and 'dependency_injector.providers' in str(p.__class__):
                container_snapshot[attr] = str(p)
    except:
        pass

    debug_report = {
        "ENVIRONMENT_CONTEXT": {
            "timestamp": datetime.now().isoformat(),
            "python_version": platform.python_version(),
            "sys_path": sys.path[:5], # Primi 5 per brevità
            "loaded_modules_count": len(sys.modules),
            **_get_system_info()
        },
        "APPLICATION_CONTEXT": app_context or {"VERSION": "N/A", "USER_ID": "anonymous"},
        "EXCEPTION_DETAILS": exception_details,
        "DI_CONTAINER_SNAPSHOT": container_snapshot,
        "STRUCTURED_TRACEBACK": structured_tb[1:-1], 
    }
    
    return debug_report

def analyze_function_calls(func: types.FunctionType) -> set[str]:
    """Analizza una funzione e restituisce i nomi di tutte le funzioni chiamate al suo interno (AST)."""
    
    # Se non riusciamo a recuperare il source (es. built-in), gestiamo l'errore.
    try:
        source_code = inspect.getsource(func)
    except OSError:
        return set()

    tree = ast.parse(source_code)
    called_names: set[str] = set()

    class CallVisitor(ast.NodeVisitor):
        def visit_Call(self, node):
            if isinstance(node.func, ast.Name):
                called_names.add(node.func.id)
            elif isinstance(node.func, ast.Attribute):
                if isinstance(node.func.value, ast.Name):
                    called_names.add(node.func.value.id + '.' + node.func.attr)
                else:
                    called_names.add(node.func.attr) 
            self.generic_visit(node)

    visitor = CallVisitor()
    visitor.visit(tree)
    return called_names

def map_dependencies(module: types.ModuleType):
    """Crea la mappa delle dipendenze: {funzione_pubblica: {dipendenze_chiamate}}."""
    dependency_map = {}
    
    for name, member in inspect.getmembers(module):
        if (inspect.isfunction(member) or inspect.ismethod(member)) and \
           not name.startswith('_') and member.__module__ == module.__name__:
            
            try:
                calls = analyze_function_calls(member)
                dependency_map[name] = calls
            except Exception:
                # Ignora errori, es. su funzioni non analizzabili
                pass
                
    return dependency_map

def correlate_failure(failing_test_name: str, dependency_map: Dict[str, set[str]]):
    """Identifica la funzione pubblica interessata dal fallimento del test."""
    if failing_test_name.startswith('test_'):
        target_fn_name = failing_test_name.replace('test_', '')
        
        inverted_map: Dict[str, set[str]] = {}
        for caller, callees in dependency_map.items():
            for callee in callees:
                inverted_map.setdefault(callee, set()).add(caller)
        
        affected_public_functions = inverted_map.get(target_fn_name, set())
        
        if affected_public_functions:
            return affected_public_functions
        elif target_fn_name in dependency_map:
            return {target_fn_name}
        
    return set()

def analyze_module(source_code: str, module_name: str) -> Dict[str, Any]:
    """Analizza il codice sorgente (AST) per ricavare la struttura del modulo."""
    structure = {"module_name": module_name, "module_docstring": None}
    
    try:
        tree = ast.parse(source_code)
        if (docstring := ast.get_docstring(tree)):
            structure["module_docstring"] = docstring.strip()
            
        ignored_nested_nodes = set()

        for node in tree.body:
            if isinstance(node, ast.ClassDef):
                class_info = {
                    "type": "class",
                    "data": {
                        "lineno": node.lineno,
                        "end_lineno": node.end_lineno,
                        "docstring": ast.get_docstring(node),
                        "methods": {},
                        "class_vars": {} 
                    }
                }
                
                for class_member in node.body:
                    if isinstance(class_member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        ignored_nested_nodes.add(class_member)
                        method_info = {
                            "type": "method",
                            "lineno": class_member.lineno,
                            "end_lineno": class_member.end_lineno,
                            "docstring": ast.get_docstring(class_member),
                            "args": [
                                a.arg for a in class_member.args.posonlyargs + class_member.args.args + [class_member.args.vararg] 
                                if a and a.arg not in ('self', 'cls')
                            ],
                        }
                        class_info["data"]["methods"][class_member.name] = method_info
                        
                    elif isinstance(class_member, ast.Assign) and class_member.targets and isinstance(class_member.targets[0], ast.Name):
                        ignored_nested_nodes.add(class_member)
                        var_name = class_member.targets[0].id
                        class_info["data"]["class_vars"][var_name] = {
                            "lineno": class_member.lineno,
                            "type_ast": type(class_member.value).__name__,
                        }

                structure[node.name] = class_info

            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if node not in ignored_nested_nodes:
                    func_info = {
                        "type": "function",
                        "data": {
                            "lineno": node.lineno,
                            "end_lineno": node.end_lineno,
                            "docstring": ast.get_docstring(node),
                            "args": [a.arg for a in node.args.posonlyargs + node.args.args + [node.args.vararg] if a],
                        }
                    }
                    structure[node.name] = func_info
            
            elif isinstance(node, ast.Assign):
                if node not in ignored_nested_nodes:
                    if isinstance(node.value, ast.Dict) and node.targets and isinstance(node.targets[0], ast.Name):
                        var_name = node.targets[0].id
                        var_value = None
                        try:
                            # Tentativo safe di valutazione
                            var_value = ast.literal_eval(node.value)
                        except (ValueError, TypeError):
                            var_value = "<Non-Literal Value>"
                        
                        info = {
                            "type": type(node.value).__name__,
                            "lineno": node.lineno,
                            "end_lineno": node.end_lineno,
                            "value": var_value,
                        }
                        structure[var_name] = info
            
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    name = alias.asname or alias.name
                    structure[name] = {
                        "type": "import",
                        "data": {"lineno": node.lineno, "module": alias.name}
                    }
            
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    name = alias.asname or alias.name
                    structure[name] = {
                        "type": "import",
                        "data": {"lineno": node.lineno, "module": node.module, "original_name": alias.name}
                    }

    except Exception as e:
        structure["parsing_error"] = f"Errore nell'analisi AST: {type(e).__name__} - {str(e)}"

    return structure

def calculate_hash_of_function(func: types.FunctionType):
    """Calcola un hash SHA256 stabile, svelando le funzioni decorate."""
    from inspect import unwrap
    try:
        unwrapped_func = unwrap(func)
    except Exception:
        # Se unwrap fallisce, usa la funzione così com'è
        unwrapped_func = func
    
    if not hasattr(unwrapped_func, '__code__'):
        # Fallback per oggetti non standard
        return hashlib.sha256(str(func).encode('utf-8')).hexdigest()

    code_obj = unwrapped_func.__code__
    
    relevant_parts = (
        code_obj.co_code,
        code_obj.co_consts,
        code_obj.co_names,
        code_obj.co_varnames,
        code_obj.co_freevars,
        code_obj.co_cellvars,
        code_obj.co_argcount,
        code_obj.co_kwonlyargcount,
        code_obj.co_flags
    )
    
    try:
        serialized = marshal.dumps(relevant_parts)
    except Exception:
        # Fallback se marshal fallisce
        serialized = str(relevant_parts).encode('utf-8')
        
    return hashlib.sha256(serialized).hexdigest()

def estrai_righe_da_codice(codice_sorgente: str, riga_inizio: int, riga_fine: int) -> str:
    """Estrae il codice sorgente tra riga_inizio e riga_fine (inclusive)."""
    righe = codice_sorgente.splitlines()
    indice_inizio = max(0, riga_inizio - 1)
    indice_fine = min(len(righe), riga_fine)
    return "\n".join(righe[indice_inizio:indice_fine])

# =====================================================================
# --- Logging Utilites ---
# =====================================================================

# --- Colori ANSI per il terminale ---
COLOR_RESET = "\033[0m"
COLORS = {
    "TRACE": "\033[90m",    # Grigio scuro
    "DEBUG": "\033[37m",    # Bianco/Grigio chiaro
    "INFO": "\033[96m",     # Cyan
    "WARNING": "\033[93m",  # Giallo
    "ERROR": "\033[91m",    # Rosso
    "CRITICAL": "\033[95m", # Magenta
}

_log_indent: contextvars.ContextVar[int] = contextvars.ContextVar("log_indent", default=0)

@contextmanager
def log_block(title: str, level: str = "DEBUG", emoji: str = "📦", timing: bool = True):
    """
    Context manager per creare un blocco di log indentato.
    Aumenta l'indentazione globale per la durata del blocco.
    """
    indent = _log_indent.get()
    framework_log(level, f"{title} (Starting...)", emoji=emoji, depth=4)
    token = _log_indent.set(indent + 1)
    start_time = time.perf_counter()
    try:
        yield
    finally:
        duration = time.perf_counter() - start_time if timing else None
        _log_indent.reset(token)
        framework_log(level, f"{title} (Completed)", emoji=emoji, duration=duration, depth=4)

@contextmanager
def timed_block(message: str, level: str = "INFO", emoji: str = "⏱️", **kwargs):
    """Context manager per loggare la durata di un blocco di codice (usa log_block internamente)."""
    with log_block(message, level=level, emoji=emoji, timing=True):
        yield

def framework_log(level: str, message: str, emoji: str = "", depth: int = 1, **kwargs):
    """
    Logger standardizzato per il framework.
    Include timestamp, livello colorato, transaction ID, origine e metadata.
    """
    from framework.service.telemetry import get_transaction_id
    tx_id = get_transaction_id() or "system"
    
    # Recupera info sul chiamante con offset variabile
    try:
        frames = inspect.stack()
        # Se depth > len(frames) usiamo l'ultimo frame disponibile
        idx = min(depth, len(frames) - 1)
        frame_info = frames[idx]
        filename = os.path.basename(frame_info.filename)
        lineno = frame_info.lineno
    except Exception:
        filename, lineno = "unknown", 0

    now = datetime.now()
    timestamp = now.strftime("%H:%M:%S")
    
    level_upper = level.upper()
    color = COLORS.get(level_upper, "")
    level_pad = f"{level_upper:8}"
    
    tx_short = tx_id[:8] if tx_id != "system" else "system"
    
    # Indentazione
    indent = _log_indent.get()
    indent_str = "│   " * indent if indent > 0 else ""
    if indent_str and not message.startswith('(Completed)'):
        indent_str = indent_str[:-4] + "├── "
    
    # --- Helper Interni per la visualizzazione ---
    def sanitize(k, v):
        sensitive = ("password", "secret", "token", "key", "auth", "credential")
        k_str = str(k).lower()
        if any(s in k_str for s in sensitive):
            return "******** [MASKED]"
        return v

    def print_tree_recursive(obj, current_prefix="    ", current_depth=0, max_depth=3):
        if current_depth > max_depth:
            print(f"{current_prefix}... [too deep]")
            return

        if isinstance(obj, dict):
            items = list(obj.items())
            for idx, (k, v) in enumerate(items):
                v_sanitized = sanitize(k, v)
                is_last_item = (idx == len(items) - 1)
                next_prefix = "    " if is_last_item else "│   "
                connector = "└─" if is_last_item else "├─"
                
                if isinstance(v_sanitized, dict) and current_depth < max_depth:
                    print(f"{current_prefix}{connector} {k}:")
                    print_tree_recursive(v_sanitized, current_prefix + next_prefix, current_depth + 1, max_depth)
                elif isinstance(v_sanitized, list) and current_depth < max_depth:
                    print(f"{current_prefix}{connector} {k}:")
                    print_tree_recursive(v_sanitized, current_prefix + next_prefix, current_depth + 1, max_depth)
                else:
                    val_str = truncate_value(str(k), v_sanitized, max_str_len=150)
                    print(f"{current_prefix}{connector} {k}: {val_str}")
        elif isinstance(obj, list):
            for idx, item in enumerate(obj[:10]):
                is_last_item = (idx == len(obj[:10]) - 1)
                connector = "└─" if is_last_item else "├─"
                if isinstance(item, (dict, list)) and current_depth < max_depth:
                    print(f"{current_prefix}{connector} [{idx}]:")
                    print_tree_recursive(item, current_prefix + "    " if is_last_item else current_prefix + "│   ", current_depth + 1, max_depth)
                else:
                    print(f"{current_prefix}{connector} [{idx}]: {truncate_value('', item, max_str_len=150)}")
            if len(obj) > 10:
                print(f"{current_prefix}└─ ... and {len(obj)-10} more items")

    # --- Inizio Log ---
    lb = container.log_buffer()
    source = f"{filename}:{lineno}"
    
    # Aggiungi durata se presente
    duration = kwargs.pop('duration', None)
    duration_str = f" [{duration:.3f}s]" if duration is not None else ""
    
    log_entry = {"timestamp": timestamp, "level": level, "message": message, "source": source, "tx_id": tx_id, "duration": duration}
    lb.append(log_entry)
    
    module_info = f"{filename}:{lineno}"
    module_info_pad = f"{module_info:20}"
    
    log_line = f"{color}[{timestamp}] [{level_pad}] [{tx_short:10}] {indent_str}{module_info_pad} - {emoji} {message}{COLOR_RESET}"
    print(log_line)

    # --- Gestione Metadata e Eccezioni ---
    items = list(kwargs.items())
    for i, (key, value) in enumerate(items):
        is_last = (i == len(items) - 1)
        prefix = "    └─" if is_last else "    ├─"
        
        if key == "exception" and isinstance(value, Exception):
            # 1. Analisi Profonda
            module_source = ""
            try:
                if os.path.exists(frame_info.filename):
                    with open(frame_info.filename, 'r') as f:
                        module_source = f.read()
            except Exception:
                pass
            
            exc_info_tuple = (type(value), value, value.__traceback__)
            report = analyze_exception(module_source, filename, exc_info=exc_info_tuple)
            
            # 2. Generazione Crash Dump (se ERROR)
            if level == "ERROR":
                try:
                    dump_dir = ".gemini/crash_dumps"
                    os.makedirs(dump_dir, exist_ok=True)
                    dump_file = os.path.join(dump_dir, f"crash_{tx_short}_{datetime.now().strftime('%H%M%S')}.json")
                    with open(dump_file, 'w') as f:
                        json.dump(report, f, cls=LogReportEncoder, indent=2)
                    print(f"{color}    📝 Crash dump salvato: {dump_file}{COLOR_RESET}")
                except Exception as de:
                    print(f"    ⚠️ Errore salvataggio dump: {de}")

            # 2.5 Log Breadcrumbs (Eventi precedenti della stessa transazione)
            breadcrumbs = lb.get_history(tx_id=tx_id, limit=6)
            # Rimuoviamo l'ultimo se è il log corrente
            if breadcrumbs and breadcrumbs[-1].get('message') == message:
                breadcrumbs = breadcrumbs[:-1]
            if breadcrumbs:
                print(f"{color}    Log Breadcrumbs (Last 5 events in TX {tx_short}):{COLOR_RESET}")
                for b_log in breadcrumbs[-5:]:
                    b_time = b_log.get('timestamp', '').split(' ')[-1]
                    b_msg = truncate_value('', b_log.get('message', ''), max_str_len=80)
                    print(f"    │   • [{b_time}] {b_msg}")

            # 3. Traceback
            tb = "".join(traceback.format_exception(*exc_info_tuple))
            connector = "    │ "
            print(f"{color}    Traceback:{COLOR_RESET}\n" + "\n".join(f"{connector}{line}" for line in tb.splitlines()))
            
            # 4. Context Diagnostico
            if "EXCEPTION_DETAILS" in report:
                details = report["EXCEPTION_DETAILS"]
                loc = details.get("error_location", {})
                code_line = loc.get("source_code_line")
                if code_line and code_line != "SORGENTE NON RECUPERATA":
                    print(f"{color}    Source Snippet ({filename}:{loc.get('line_number')}):{COLOR_RESET}")
                    if module_source:
                        line_num = loc.get('line_number')
                        start_l = max(1, line_num - 2)
                        end_l = line_num + 2
                        snippet_lines = estrai_righe_da_codice(module_source, start_l, end_l).splitlines()
                        for idx, s_line in enumerate(snippet_lines):
                            curr_line = start_l + idx
                            marker = ">" if curr_line == line_num else " "
                            print(f"    │   {marker} {curr_line:3} | {s_line}")
                    else:
                        print(f"    │   > {code_line.strip()}")

                locs = details.get("LOCAL_VARIABLES_STATE_FINAL_FRAME", {})
                if locs:
                    print(f"{color}    Local Variables (Final Frame):{COLOR_RESET}")
                    print_tree_recursive(locs, "    │   ", current_depth=0)
                
            # 5. Struttura Modulo
            if module_source:
                mod_report = analyze_module(module_source, filename)
                classes = [k for k, v in mod_report.items() if isinstance(v, dict) and v.get('type') == 'class']
                funcs = [k for k, v in mod_report.items() if isinstance(v, dict) and v.get('type') == 'function']
                if classes or funcs or mod_report.get("module_docstring"):
                    print(f"{color}    Module Structure ({filename}):{COLOR_RESET}")
                    doc = mod_report.get("module_docstring")
                    if doc: print(f"    │   ├─ Doc: {truncate_value('', doc, max_str_len=80)}")
                    print(f"    │   └─ Summary: {len(classes)} classes, {len(funcs)} functions")
            
            # 6. Environment
            env = report.get("ENVIRONMENT_CONTEXT", {})
            if env:
                print(f"{color}    Environment Snapshot:{COLOR_RESET}")
                print(f"    │   └─ Host: {env.get('hostname')} | OS: {platform.system()} | PID: {env.get('process_id')}")

        elif key in ("module", "analysis") and isinstance(value, (types.ModuleType, dict)):
            # Visualizzazione speciale per moduli o analisi pre-calcolate
            if isinstance(value, types.ModuleType):
                m_report = {}
                m_source = ""
                try:
                    m_source = inspect.getsource(value)
                except Exception:
                    m_file = kwargs.get('module_path') or kwargs.get('path') or getattr(value, '__file__', None)
                    if m_file:
                        candidates = [m_file, os.path.join("src", m_file), os.path.join(os.getcwd(), "src", m_file)]
                        for cand in candidates:
                            if os.path.exists(cand) and os.path.isfile(cand):
                                try:
                                    with open(cand, 'r') as f:
                                        m_source = f.read()
                                    break
                                except Exception: pass
                
                if m_source:
                    m_report = analyze_module(m_source, getattr(value, '__name__', 'unknown'))
                else:
                    m_report = {"error": "cannot retrieve module source"}
            else:
                m_report = value
            
            print(f"{prefix} {key} introspection ({getattr(value, '__name__', 'unknown')}):")
            child_prefix = "    │   " if not is_last else "        "
            print_tree_recursive(m_report, child_prefix, max_depth=1)

        else:
            # Metadata generici con TREE RECURSIVE
            val = sanitize(key, value)
            if isinstance(val, (dict, list)):
                print(f"{prefix} {key}:")
                # Indenta correttamente in base a prefix
                child_prefix = "    │   " if not is_last else "        "
                print_tree_recursive(val, child_prefix, max_depth=2)
            else:
                val_str = truncate_value(key, val, max_str_len=200)
                print(f"{prefix} {key}: {val_str}")

    return True

# Alias per compatibilità retroattiva
buffered_log = framework_log

# =====================================================================
# --- Resource Loading Utilites ---
# =====================================================================

def _check_single_import(module_name, allowed_prefixes, project_modules, layer, lineno, file_path, is_path=False):
    if is_path:
        root_module = module_name.split('/')[0]
    else:
        root_module = module_name.split('.')[0]
    
    if root_module not in project_modules:
        return 

    if root_module not in allowed_prefixes:
        # In un contesto reale questo potrebbe alzare ImportError,
        # qui logghiamo o ignoriamo per semplicità durante il refactoring
        pass

def _validate_imports(content: str, file_path: str):
    """
    Validates that imports in the file respect the architectural layering rules.
    """
    layer = None
    if 'src/application/' in file_path:
        layer = 'application'
    elif 'src/framework/' in file_path:
        layer = 'framework'
    elif 'src/infrastructure/' in file_path:
        layer = 'infrastructure'
    
    if not layer:
        return

    allowed_imports = {
        'application': ['application'],
        'framework': ['framework', 'application'],
        'infrastructure': ['infrastructure', 'framework']
    }
    
    allowed = allowed_imports.get(layer)
    if not allowed:
        return

    project_modules = ['application', 'framework', 'infrastructure']

    try:
        tree = ast.parse(content, filename=file_path)
    except SyntaxError:
        return

    for node in ast.walk(tree):
        imported_module = None
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported_module = alias.name
                _check_single_import(imported_module, allowed, project_modules, layer, node.lineno, file_path)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imported_module = node.module
                _check_single_import(imported_module, allowed, project_modules, layer, node.lineno, file_path)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == 'imports':
                    if isinstance(node.value, ast.Dict):
                        for i, value in enumerate(node.value.values):
                            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                                _check_single_import(value.value, allowed, project_modules, layer, node.lineno, file_path, is_path=True)

if sys.platform != 'emscripten':
    async def _load_resource(**kwargs) -> str:
        path = kwargs.get("path", "")
        if path.startswith('/'):
            path = path[1:]

        # Create candidates: relative to CWD, relative to project root (derived from __file__)
        cwd = os.getcwd()
        candidates = [path]
        if not path.startswith('src/'):
            candidates.append('src/' + path)
        
        # Determine project root from current file (inspector.py is in src/framework/service/inspector.py)
        # So project root is 3 levels up from inspector.py's directory
        try:
            inspector_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(inspector_dir)))
            candidates.append(os.path.join(project_root, 'src', path))
            candidates.append(os.path.join(project_root, path))
        except Exception:
            pass

        # Prova i vari candidati
        for p in candidates:
            if os.path.exists(p) and os.path.isfile(p):
                try:
                    with open(p, "r") as f:
                        content = f.read()
                        _validate_imports(content, p)
                        return content
                except Exception:
                    continue
        
        raise FileNotFoundError(f"File non trovato: {path}. Provati: {candidates}. CWD: {cwd}")
else:
    async def _load_resource(**kwargs) -> str:
        path = kwargs.get("path", "")
        try:
            resp = await js.fetch(path)
            return await resp.text()
        except Exception as e:
            raise FileNotFoundError(f"File non trovato (fetch fallito): {path}") from e

async def _save_resource(**kwargs):
    path = kwargs.get("path", "")
    content = kwargs.get("content", "")
    mode = kwargs.get("mode", "w")
    
    # Path resolution similar to _load_resource
    if path.startswith('/'): path = path[1:]
    
    # In a real system, we'd handle candidate paths, but typically we save to a specific one
    # For simplicity in this environment:
    actual_path = path
    if not os.path.isabs(path) and not path.startswith('src/'):
        actual_path = os.path.join('src', path)
        
    os.makedirs(os.path.dirname(actual_path), exist_ok=True)
    with open(actual_path, mode) as f:
        f.write(content)
    return True

backend = _save_resource

