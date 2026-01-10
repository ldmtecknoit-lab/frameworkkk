
import json
import tomli
import hashlib
import copy
from urllib.parse import urlparse, urlencode
from jinja2 import Environment
from cerberus import Validator
from framework.service.inspector import LogReportEncoder, framework_log, buffered_log, _load_resource

mappa = {
    (str,dict,''): lambda v: v if isinstance(v, dict) else {},
    (str,dict,'json'): lambda v: json.loads(v) if isinstance(v, str) else {},
    (dict,str,'json'): lambda v: json.dumps(v,indent=4,cls=LogReportEncoder) if isinstance(v, dict) else '',
    (str,str,'hash'): lambda v: hashlib.sha256(v.encode('utf-8')).hexdigest() if isinstance(v, str) else '',
    (str,dict,'toml'): lambda content: tomli.loads(content) if isinstance(content, str) else {},
    (dict,str,'toml'): lambda data: tomli.dumps(data) if isinstance(data, dict) else '',
}

async def convert(target, output,input=''):
    try:
        return mappa[(type(target),output,input)](target)
    except KeyError:
        raise ValueError(f"Conversione non supportata: {type(target)} -> {type(output)} da {input}")
    except Exception as e:
        raise ValueError(f"Errore conversione: {e}")

def get(data, path, default=None):
    if not path: return data
    
    parts = path.split('.', 1)
    key_str = parts[0]
    rest = parts[1] if len(parts) > 1 else None

    # Gestione Wildcard
    if key_str == '*':
        if isinstance(data, list):
            return [get(item, rest or '', default) for item in data]
        return default

    # Accesso Sicuro
    next_data = default
    try:
        if isinstance(data, (list, tuple)):
            # Solo per le liste/tuple convertiamo in int
            if key_str.lstrip('-').isnumeric():
                next_data = data[int(key_str)]
        elif isinstance(data, dict):
            # Per i dict usiamo la chiave stringa originale
            next_data = data.get(key_str)
        else:
             # Opzionale: Aggiungere qui getattr per oggetti se serve
             next_data = getattr(data, key_str, default)
    except (IndexError, TypeError):
        return default

    if rest is None:
        return next_data if next_data is not None else default
    return get(next_data, rest, default)

async def format(target ,**constants):
    try:
        jinjaEnv = Environment()
        jinjaEnv.filters['get'] = lambda d, k, default=None: d.get(k, default) if isinstance(d, dict) else default
        template = jinjaEnv.from_string(target)
        return template.render(constants)
    except Exception as e:
        raise ValueError(f"Errore formattazione: {e}")

async def normalize(value, schema, mode='full'):
    """
    Convalida, popola, trasforma e struttura i dati utilizzando uno schema Cerberus.
    """
    value = value or {}

    if not isinstance(schema, dict):
        raise TypeError("Lo schema deve essere un dizionario valido per Cerberus.",schema)
    if not isinstance(value, dict):
        raise TypeError("I dati devono essere un dizionario valido per Cerberus.",value)

    # 1. Popolamento e Trasformazione Iniziale
    processed_value = value
    for key in schema.copy():
        item = schema[key]
        for field_name, field_rules in item.copy().items():
            if field_name.startswith('_'):
                schema.get(key).pop(field_name)

    for field_name, field_rules in schema.copy().items():
        if isinstance(field_rules, dict) and 'function' in field_rules:
            func_name = field_rules['function']
            if func_name == 'generate_identifier':
                if field_name not in processed_value:
                    pass
            elif func_name == 'time_now_utc':
                if field_name not in processed_value:
                    pass

    # Cerberus Validation
    v = Validator(schema,allow_unknown=True)

    if not v.validate(processed_value):
        framework_log("WARNING", f"Errore di validazione: {v.errors}", emoji="⚠️", data=processed_value)
        raise ValueError(f"⚠️ Errore di validazione: {v.errors} | data:{processed_value}")

    final_output = v.document
    return final_output

def transform(data_dict, mapper, values, input, output):
    """ Trasforma un set di costanti in un output mappato. """
    def find_matching_keys(mapper, target_dict):
        if not isinstance(mapper, dict) or not isinstance(target_dict, dict):
            return None
        target_keys = set(target_dict.keys())
        for key in mapper.keys():
            if key in target_keys:
                return key
        return None
    translated = {}

    if not isinstance(data_dict, dict):
        raise TypeError("Il primo argomento deve essere un dizionario.")

    if not isinstance(mapper, dict):
        raise TypeError("'mapper' deve essere un dizionario.")

    if not isinstance(values, dict):
        raise TypeError("'values' deve essere un dizionario.")
    
    if not isinstance(input, dict):
        raise TypeError("'input' deve essere un dizionario.")
    
    if not isinstance(output, dict):
        raise TypeError("'output' deve essere un dizionario.")

    key = find_matching_keys(mapper,output) or find_matching_keys(mapper,input)
    for k, v in mapper.items():
        n1 = get(data_dict, k)
        n2 = get(data_dict, v.get(key, None))
        
        if n1:
            output_key = v.get(key, None)
            value = n1
            translated |= put(translated, output_key, value, output)
        if n2:
            output_key = k
            value = n2
            translated |= put(translated, output_key, value, output)

    fieldsData = data_dict.keys()
    fieldsOutput = output.keys()

    for field in fieldsData:
        if field in fieldsOutput:
            value = get(data_dict, field)
            translated |= put(translated, field, value, output)

    return translated

def _get_next_schema(schema, key):
    if isinstance(schema, dict):
        if 'schema' in schema:
            if schema.get('type') == 'list': return schema['schema']
            if isinstance(schema['schema'], dict): return schema['schema'].get(key)
        return schema.get(key)
    return None

def put(data: dict, path: str, value: any, schema: dict) -> dict:
    if not isinstance(data, dict): raise TypeError("Il dizionario iniziale deve essere di tipo dict.")
    if not isinstance(path, str) or not path: raise ValueError("Il dominio deve essere una stringa non vuota.")
    if not isinstance(schema, dict) or not schema: raise ValueError("Lo schema deve essere un dizionario valido.")

    result = copy.deepcopy(data)
    node, sch = result, schema
    chunks = path.split('.')

    for i, chunk in enumerate(chunks):
        is_last = i == len(chunks) - 1
        is_index = chunk.lstrip('-').isdigit()
        key = int(chunk) if is_index else chunk
        next_sch = _get_next_schema(sch, chunk)

        if isinstance(node, dict):
            if is_index:
                raise IndexError(f"Indice numerico '{chunk}' usato in un dizionario a livello {i}.")
            if is_last:
                if next_sch is None:
                    raise IndexError(f"Campo '{chunk}' non definito nello schema.")
                if not Validator({chunk: next_sch}, allow_unknown=False).validate({chunk: value}):
                    raise ValueError(f"Valore non valido per '{chunk}': {value}")
                node[key] = value
            else:
                node.setdefault(key, {} if next_sch and next_sch.get('type') == 'dict'
                                     else [] if next_sch and next_sch.get('type') == 'list'
                                     else None)
                if node[key] is None:
                    raise IndexError(f"Nodo intermedio '{chunk}' non valido nello schema.")
                node, sch = node[key], next_sch

        elif isinstance(node, list):
            if not is_index:
                raise IndexError(f"Chiave '{chunk}' non numerica usata in una lista a livello {i}.")
            if not isinstance(next_sch, dict) or 'type' not in next_sch:
                raise IndexError(f"Schema non valido per lista a livello {i}.")

            if key == -1:  # Append mode
                t = next_sch['type']
                new_elem = {} if t == 'dict' else [] if t == 'list' else None
                node.append(new_elem)
                key = len(node) - 1

            if key < 0:
                raise IndexError(f"Indice negativo '{chunk}' non valido in lista.")

            while len(node) <= key:
                t = next_sch['type']
                node.append({} if t == 'dict' else [] if t == 'list' else None)

            if is_last:
                if not Validator({chunk: next_sch}, allow_unknown=False).validate({chunk: value}):
                    raise ValueError(f"Valore non valido per indice '{chunk}': {value}")
                node[key] = value
            else:
                if node[key] is None or not isinstance(node[key], (dict, list)):
                    t = next_sch['type']
                    if t == 'dict': node[key] = {}
                    elif t == 'list': node[key] = []
                    else: raise IndexError(f"Tipo non contenitore '{t}' per nodo '{chunk}' in lista.")
                node, sch = node[key], next_sch

        else:
            raise IndexError(f"Nodo non indicizzabile al passo '{chunk}' (tipo: {type(node).__name__})")

    return result

def route(url: dict, new_part: str) -> str:
    """
    Updates the URL's path and/or adds query parameters based on the input string.
    """
    url = copy.deepcopy(url)
    protocol = url.get("protocol", "http")
    host = url.get("host", "localhost")
    port = url.get("port")
    path = url.get("path", [])
    query_params = url.get('query', {})
    fragment = url.get("fragment", "")

    parsed_new_part = urlparse(new_part)

    if parsed_new_part.path:
        path = [p for p in parsed_new_part.path.split('/') if p]

    if parsed_new_part.query:
        [query_params.setdefault(k, []).append(v) for k, v in (param.split('=', 1) for param in parsed_new_part.query.split('&') if '=' in param)]
        for key, value in query_params.items():
            pass
    
    query_parts = []
    query_string = ""
    for key, values in query_params.items():
        if values:  # prendi solo l'ultimo elemento
            query_parts.append(f"{key}={values[-1]}")
    query_string = "&".join(query_parts)

    base_url = ""
    if path:
        base_url += "/" + "/".join(path)

    if query_string:
        base_url += f"?{query_string}"
    
    if fragment:
        base_url += f"#{fragment}"

    return base_url
