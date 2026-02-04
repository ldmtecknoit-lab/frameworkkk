
import json
import tomli
import hashlib
import copy
from urllib.parse import urlparse, urlencode
from jinja2 import Environment
from cerberus import Validator
from framework.service.diagnostic import LogReportEncoder, framework_log, buffered_log, _load_resource

mappa = {
    (str,dict,''): lambda v: v if isinstance(v, dict) else {},
    (dict,dict,''): lambda v: v,
    (str,str,''): lambda v: v,
    (str,dict,'json'): lambda v: json.loads(v) if isinstance(v, str) else v if isinstance(v, dict) else {},
    (dict,dict,'json'): lambda v: v,
    (dict,str,'json'): lambda v: json.dumps(v,indent=4,cls=LogReportEncoder) if isinstance(v, dict) else v if isinstance(v, str) else '',
    (str,str,'json'): lambda v: v,
    (str,str,'hash'): lambda v: hashlib.sha256(v.encode('utf-8')).hexdigest() if isinstance(v, str) else '',
    (str,dict,'toml'): lambda content: tomli.loads(content) if isinstance(content, str) else content if isinstance(content, dict) else {},
    (dict,dict,'toml'): lambda v: v,
    (dict,str,'toml'): lambda data: tomli.dumps(data) if isinstance(data, dict) else data if isinstance(data, str) else '',
    (str,str,'toml'): lambda v: v,
    (str,int,''): lambda v: int(v) if isinstance(v, str) else v if isinstance(v, int) else 0,
    (int,str,''): lambda v: str(v) if isinstance(v, int) else v if isinstance(v, str) else '',
    (str,bool,''): lambda v: True if v.lower() == 'true' else False,
    (bool,str,''): lambda v: str(v) if isinstance(v, bool) else v if isinstance(v, str) else '',
    (str,list,''): lambda v: [v],
    (type(None),list,''): lambda v: [],
}

async def convert(target, output,input=''):
    try:
        return mappa[(type(target),output,input)](target)
    except KeyError:
        raise ValueError(f"Conversione non supportata: {type(target)} -> {type(output)}:{output} da {input}")
    except Exception as e:
        raise ValueError(f"Errore conversione: {e}")

def get(data, path, default=None):
    result = default

    if path:
        key, _, rest = path.partition(".")

        if key == "*" and isinstance(data, (list, tuple)):
            result = [get(x, rest, default) for x in data]

        else:
            try:
                if isinstance(data, (list, tuple)):
                    if key.lstrip("-").isdigit():
                        value = data[int(key)]
                    else:
                        value = default
                elif isinstance(data, dict):
                    value = data.get(key, default)
                else:
                    value = getattr(data, key, default)

                if rest and value is not default:
                    result = get(value, rest, default)
                else:
                    result = value

            except (IndexError, TypeError, ValueError):
                result = default

    else:
        result = data

    return result

async def format(target ,**constants):
    try:
        jinjaEnv = Environment()
        jinjaEnv.filters['get'] = lambda d, k, default=None: d.get(k, default) if isinstance(d, dict) else default
        template = jinjaEnv.from_string(target)
        return template.render(constants)
    except Exception as e:
        raise ValueError(f"Errore formattazione: {e}")

async def normalize(value, schema, mode='full'):
    def to_list(value):
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]
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
    '''for key in schema.copy():
        item = schema[key]
        for field_name, field_rules in item.copy().items():
            if field_name.startswith('_'):
                schema.get(key).pop(field_name)'''

    for field_name, field_rules in schema.copy().items():
        value = processed_value.get(field_name)
        if isinstance(field_rules, dict) and 'function' in field_rules:
            func_name = field_rules['function']
            if func_name == 'generate_identifier':
                if field_name not in processed_value:
                    pass
            elif func_name == 'time_now_utc':
                if field_name not in processed_value:
                    pass
        if isinstance(field_rules, dict) and "_convert" in field_rules:
            convert_name = field_rules["_convert"]
            print("convert_name",convert_name)
            print("processed_value",value)

            if field_name in processed_value:
                processed_value[field_name] = await convert(value, convert_name)

            schema[field_name].pop("_convert")

    # Cerberus Validation
    v = Validator(schema,allow_unknown=True)
    v.coerce = {
        "to_list": to_list
    }

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
            translated |= put2(translated, output_key, value, output)
        if n2:
            output_key = k
            value = n2
            translated |= put2(translated, output_key, value, output)

    fieldsData = data_dict.keys()
    fieldsOutput = output.keys()

    for field in fieldsData:
        if field in fieldsOutput:
            value = get(data_dict, field)
            translated |= put2(translated, field, value, output)

    return translated

def _get_next_schema(schema, key):
    if isinstance(schema, dict):
        if 'schema' in schema:
            if schema.get('type') == 'list': return schema['schema']
            if isinstance(schema['schema'], dict): return schema['schema'].get(key)
        return schema.get(key)
    return None

def put2(data: dict, path: str, value: any, schema: dict) -> dict:
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

def put(data: dict, path: str, value) -> dict:
    if not isinstance(data, dict):
        raise TypeError("data deve essere un dict")
    if not path or not isinstance(path, str):
        raise ValueError("path non valido")

    res = copy.deepcopy(data)
    node = res
    parts = path.split(".")

    for i, part in enumerate(parts):
        last = i == len(parts) - 1
        is_idx = part.lstrip("-").isdigit()
        key = int(part) if is_idx else part

        if isinstance(node, dict):
            if is_idx:
                raise IndexError(f"indice '{part}' su dict")

            if last:
                node[key] = value
            else:
                nxt = node.get(key)
                if not isinstance(nxt, (dict, list)):
                    nxt = {} if not is_idx else []
                    node[key] = nxt
                node = nxt

        elif isinstance(node, list):
            if not is_idx:
                raise IndexError(f"chiave '{part}' su lista")

            if key == -1:
                node.append({})
                key = len(node) - 1

            if key < 0:
                raise IndexError("indice negativo")

            while len(node) <= key:
                node.append({})

            if last:
                node[key] = value
            else:
                if not isinstance(node[key], (dict, list)):
                    node[key] = {}
                node = node[key]

        else:
            raise IndexError(f"nodo non indicizzabile: {type(node).__name__}")

    return res