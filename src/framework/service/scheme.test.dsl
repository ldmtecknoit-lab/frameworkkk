# Test per la funzione get
data: {
    "nome": "Progetto A";
    "versioni": (
        {"id": 1; "status": "completo";},
        {"id": 2; "status": "in_corso"; "dettagli": {"tester": "Mario";};},
        {"id": 3; "status": "fallito";}
    );
    "config": {
        "timeout": 30;
        "log_livello": "DEBUG";
    };
};

dict:utente_schema := {
  "nome": {
    "type": "string";
    "required": False;
  };
  "cognome": {
    "type": "string";
    "required": True;
  };
  "eta": {
    "type": "number";
    "required": True;
  };
  "email": {
    "type": "string";
    "required": False;
    "nullable": True;
  };
  "numero": {
    "type": "number";
    "required": True;
    "min": 0;
  };
  "indirizzo": {
    "type": "string";
    "required": False;
    "nullable": True;
  };
};

dict:user_schema := {
  "name": {
    "type": "string";
    "required": False;
  };
  "surname": {
    "type": "string";
    "required": True;
  };
  "age": {
    "type": "number";
    "required": True;
  };
  "email": {
    "type": "string";
    "required": False;
    "nullable": True;
  };
  "phone": {
    "type": "number";
    "required": True;
    "min": 0;
  };
  "address": {
    "type": "string";
    "required": False;
    "nullable": True;
  };
};

# Test per la funzione get
str:get_1 := get(data, "nome");
int:get_2 := get(data, "config.timeout");
str:get_3 := get(data, "versioni.0.status");
str:get_4 := get(data, "versioni.1.dettagli.tester");
list:get_5 := get(data, "versioni.*.status");
list:get_6 := get(data, "versioni.*.id");

# Test per la funzione format
str:format_1 := format("Ciao {{nome}}", nome: "Progetto A");

# Convert
int:convert_1 := convert("10", int);
str:convert_2 := convert(10, str);
bool:convert_3 := convert("true", bool);
bool:convert_4 := convert("false", bool);
str:convert_5 := convert(true, str);
str:convert_6 := convert(false, str);
#str:convert_7 := convert(True, bool);
#str:convert_8 := convert(False, bool);

# put 
dict:put_1 := put(data, "nome", "Progetto B");
#dict:put_2 := put(data, "versioni.1.status", "completo");
dict:put_3 := put(data, "config.timeout", 60);
#dict:put_4 := put(data, "versioni.*.status", "completo");
#dict:put_5 := put(data, "versioni.*.dettagli.tester", "Mario");

# normalize
dict:normalize_1 := normalize({
    "name": "Mario";
    "surname": "Rossi";
    "age": 30;
    "email": "[EMAIL_ADDRESS]";
    "phone": 1234567890;
    "address": "Via Roma 1";
}, user_schema);

# transform 
any:transform_1 := transform({
    "name": "Mario";
    "surname": "Rossi";
    "age": 30;
    "email": "[EMAIL_ADDRESS]";
    "phone": 1234567890;
    "address": "Via Roma 1";
}, { name: { model:"name"; user:"nome"; }; age: { model:"age"; user:"eta"; }; output: 30; }, { }, user_schema, utente_schema);

# Test suite
tuple:test_suite := (
    { target: 'normalize_1'; output: {"name": "Mario"; "surname": "Rossi"; "age": 30; "email": "[EMAIL_ADDRESS]"; "phone": 1234567890; "address": "Via Roma 1";}; },
    { target: 'get_1'; output: "Progetto A"; },
    { target: 'get_2'; output: 30; },
    { target: 'get_3'; output: "completo"; },
    { target: 'get_4'; output: "Mario"; },
    { target: 'get_5'; output: ["completo", "in_corso", "fallito"]; },
    { target: 'get_6'; output: [1, 2, 3]; },
    { target: 'format_1'; output: "Ciao Progetto A"; },
    { target: 'convert_1'; output: 10; },
    { target: 'convert_2'; output: "10"; },
    { target: 'convert_3'; output: True; },
    { target: 'convert_4'; output: False; },
    { target: 'convert_5'; output: "True"; },
    { target: 'convert_6'; output: "False"; },
    { target: 'convert_7'; output: True; },
    { target: 'convert_8'; output: False; },
    { target: 'put_1'; output: merge(data,{"nome": "Progetto B"; }); },
    { target: 'put_3'; output: {
        "nome": "Progetto A";
        "versioni": (
            {"id": 1; "status": "completo";},
            {"id": 2; "status": "in_corso"; "dettagli": {"tester": "Mario";};},
            {"id": 3; "status": "fallito";}
        );
        "config": {
            "timeout": 60;
            "log_livello": "DEBUG";
        };
    }; },
);