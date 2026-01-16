import re
import framework.service.language as language
from framework.service.diagnostic import framework_log

class repository():
    def __init__(self, **constants):
        self.location = constants.get('location',{})
        self.mapper = constants.get('mapper',{})
        self.values = constants.get('values',{})
        self.payloads = constants.get('payloads',{})
        self.functions = constants.get('functions',{})
        self.schema = constants.get('model')

    def can_format(self,template, data):
            """
            Verifica se una singola stringa `template` può essere formattata utilizzando le chiavi di un dizionario `data`.
            """
            try:
                placeholders = re.findall(r'\{([\w\.]+)\}', template)
                gg = []
                for key in placeholders:
                    a = language.get(data,key)
                    framework_log("DEBUG", f"Checking placeholder: {key} -> {a}", emoji="🔍")
                    if a:
                        gg.append(True)
                    else:
                         gg.append(False)

                return (all(gg),len(placeholders))
            except Exception as e:
                framework_log("ERROR", f"Errore durante la verifica template: {e}", emoji="❌")
                return False
            
    def do_format(self,template, data):
            """
            Verifica se una singola stringa `template` può essere formattata utilizzando le chiavi di un dizionario `data`.
            """
            try:
                placeholders = re.findall(r'\{([\w\.]+)\}', template)
                framework_log("DEBUG", f"Formatting template: {template}", emoji="📝", placeholders=placeholders)
                for key in placeholders:
                    a = language.get(key,data)
                    if a:
                        template = template.replace(f'{key}',str(a))
                return template
            except Exception as e:
                framework_log("ERROR", f"Errore durante il formattazione: {e}", emoji="❌")
                return False
            
    def find_first_formattable_template(self, templates, data):
        """
        Trova il template con il più alto numero di placeholder formattabili e con True.
        """
        best_template = None
        max_placeholders = 0
        for template in templates:
            can_format_result, num_placeholders = self.can_format(template, data)
            framework_log("DEBUG", f"Template evaluation: {template}", state=can_format_result, count=num_placeholders)
            if can_format_result and num_placeholders >= max_placeholders:
                best_template = template
                max_placeholders = num_placeholders
        return best_template

    async def results(self, **data):
        framework_log("DEBUG", "Processing results", emoji="📊", data=data)
        try:
            profile = data.get('profile', '')
            transaction = data.get('transaction', {})
            results = transaction.get('result', [])

            if not isinstance(results, list):
                raise ValueError("Il campo 'result' deve essere una lista.")

            r = []
            for item in results:
                if isinstance(item, dict):
                    try:
                        r.append(item)
                    except Exception as e:
                        framework_log("WARNING", f"Errore durante la traduzione dell'elemento: {e}", item=item)
                        continue 

            transaction['result'] = r
            data['transaction'] = transaction
            return transaction

        except KeyError as e:
            framework_log("ERROR", f"Chiave mancante nei dati: {e}", emoji="❌")
            raise
        except Exception as e:
            framework_log("ERROR", f"Errore generico in 'results': {e}", emoji="❌")
            raise
    
    async def parameters(self, ops_crud, profile, **inputs) -> object:
        try:
            framework_log("DEBUG", f"Computing parameters for profile: {profile}", emoji="🔧", schema=self.schema)

            payload = inputs.get('payload', {})
            para = {}

            func_payload = self.payloads.get(ops_crud, None)
            if func_payload:
                payload = await func_payload(**inputs)

            func_payload = self.functions.get(ops_crud, None)
            if func_payload:
                para = await func_payload(**inputs)

            combined_parameters = {**inputs, **payload}
            framework_log("DEBUG", "Combined parameters for template selection", params=combined_parameters)

            templates = self.location.get(profile, [''])
            template = self.find_first_formattable_template(templates, combined_parameters)
            if not template:
                raise ValueError(f"Nessun template formattabile trovato per il profilo: {profile}")
            framework_log("DEBUG", f"Selected template: {template}", emoji="🎯")

            path = await language.format(template,**combined_parameters)
            framework_log("INFO", f"Generated location path: {path}", emoji="🌐")
            
            return para|{**inputs, 'location': path, 'provider': profile, 'payload': payload}

        except Exception as e:
            framework_log("ERROR", f"Errore in parameters: {e}", emoji="❌")
            raise