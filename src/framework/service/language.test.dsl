{
    # --- 1. GRAMMATICA BASE E TIPI ---
    int:base_int := 10;
    float:base_float := 20.5;
    str:base_str := "Ciao";
    str:base_str_single := 'Mondo';
    boolean:b1 := Vero;
    boolean:b2 := False;
    any:qualsiasi := *;

    # --- 2. OPERATORI ARITMETICI (Precedenza e Parentesi) ---
    int:math_1 := 2 + 3 * 4;       # 14
    int:math_2 := (2 + 3) * 4;     # 20
    int:math_3 := 10 / 2 - 1;      # 4
    int:math_4 := 10 % 3;          # 1
    int:math_5 := 2 ^ 3;           # 8
    
    # --- 3. OPERATORI LOGICI E COMPARAZIONE ---
    boolean:logic_1 := Vero & (10 > 5) or Falso; # Vero
    boolean:logic_2 := not (5 == 5) or (1 != 2); # Vero
    boolean:logic_3 := (10 >= 10) & (5 <= 6) & (2 < 3) & (4 > 1); # Vero
    
    # --- 4. COLLEZIONI (Liste, Dizionari, Tuple) ---
    list:lista_semplice := [1, 2, "tre", Vero];
    dict:diz_semplice := { "chiave": "valore"; "num": 42; };
    tuple:tupla_semplice := (1, "test"); 
    tuple:tupla_inline := 1, 2, 3; # Test tuple_inline senza parentesi
    list:lista_trailing := [1, 2]; 
    dict:diz_trailing := { "a": 1; "b": 2; };

    # --- 5. FUNZIONI (Definizione, Chiamata, Ritorno Multiplo) ---
    function:f_base := (int:n), { 
        res: n * 2; 
    }, (int:res);
    
    function:f_multi_in := (int:x, int:y), { 
        somma: x + y; 
    }, (int:somma);
    
    function:f_multi_out := (int:val), {
        v1: val + 1;
        v2: val + 2;
    }, (int:v1, int:v2);
    
    int:test_f1 := f_base(5);                # 10
    int:test_f2 := f_multi_in(10, 20);       # 30
    tuple:test_f3 := f_multi_out(100);       # (101, 102)
    
    # Chiamata con argomenti keyword
    int:test_kw := f_multi_in(y: 50, x: 10); # 60

    # --- 6. PIPE E ESPRESSIONI AVANZATE ---
    int:test_pipe := 10 |> f_base |> f_base; # 10 -> 20 -> 40
    
    # Pipe con parametri extra
    int:test_pipe_extra := 10 |> f_multi_in(5); # 15

    # --- 7. LIBRERIA STANDARD (flow.py) ---
    dict:lib_merge := merge({ "a": 1; }, { "b": 2; });
    list:lib_concat := concat([1], [2]);
    dict:lib_pick := { "a": 1; "b": 2; "c": 3; } |> pick(["a", "c"]);
    
    list:lib_keys := keys({ "x": 1; "y": 2; });
    list:lib_values := values({ "x": 1; "y": 2; });
    
    str:lib_format := format("Hello {name}", name: "World");
    #str:lib_convert := 123 |> convert(str);
    
    list:lib_map := [1, 2, 3] |> map("@ * 2"); # [2, 4, 6]
    #list:lib_foreach := [10, 20] |> foreach((int:v), { r: v + 5; }, (int:r)); # [15, 25]
    
    dict:lib_project := { "nested": { "val": 99; }; } |> project({ "out": "@.nested.val"; });
    
    int:lib_query := { "data": [10, 20, 30]; } |> query("data[1]"); # 20

    # --- 8. CONTROLLO DI FLUSSO (Switch/Match) ---
    str:test_match := 75 |> match({
        "@ > 90": "Ottimo";
        "@ > 60": "Sufficiente";
        "*": "Insufficiente";
    });

    # --- 9. QUALIFIED NAMES E DOT NOTATION ---
    dict:servizio := { 
        "config": { "timeout": 30; };
        "azione": (int:x), { r: x + 1; }, (int:r);
    };
    #int:test_dot_1 := servizio.config.timeout; # 30
    #int:test_dot_2 := servizio.azione(9);      # 10

    # --- 10. INCLUDE ---
    #dict:test_include_res := include("src/framework/service/dependency.dsl");
    #str:check_include_var := valore_incluso;
    #int:check_include_func := raddoppia(5);

    # --- 11. EDGE CASES ---
    (p1: 100); # Mapping tra parentesi
    
    # --- 12. TEST SUITE COMPLETA ---
    list:test_suite := [
        { "target": "math_1"; "expected_output": 14; "description": "Moltiplicazione prima di addizione"; },
        { "target": "math_2"; "expected_output": 20; "description": "Parentesi forzano addizione prima"; },
        { "target": "math_5"; "expected_output": 8; "description": "Potenza 2^3"; },
        { "target": "logic_1"; "expected_output": Vero; "description": "Logica AND/OR"; },
        { "target": "logic_2"; "expected_output": Vero; "description": "Logica NOT"; },
        { "target": "tupla_semplice"; "expected_output": (1, "test"); "description": "Tupla definita senza parentesi (semplice)"; },
        { "target": "tupla_inline"; "expected_output": (1, 2, 3); "description": "Tupla definita senza parentesi (inline)"; },
        { "target": "test_f1"; "expected_output": 10; "description": "Chiamata funzione base"; },
        { "target": "test_f3"; "expected_output": (101, 102); "description": "Ritorno multiplo"; },
        { "target": "test_kw"; "expected_output": 60; "description": "Keyword arguments"; },
        { "target": "test_pipe"; "expected_output": 40; "description": "Chaining di pipe"; },
        { "target": "test_pipe_extra"; "expected_output": 15; "description": "Pipe con argomenti extra"; },
        { "target": "lib_merge"; "expected_output": { "a": 1; "b": 2; }; "description": "Standard merge"; },
        { "target": "lib_pick"; "expected_output": { "a": 1; "c": 3; }; "description": "Standard pick"; },
        { "target": "lib_keys"; "expected_output": ["x", "y"]; "description": "Standard keys"; },
        { "target": "lib_format"; "expected_output": "Hello World"; "description": "Standard format"; },
        { "target": "lib_map"; "expected_output": [2, 4, 6]; "description": "MistQL map"; },
        { "target": "lib_query"; "expected_output": 20; "description": "MistQL query"; },
        { "target": "test_match"; "expected_output": "Sufficiente"; "description": "Controllo flusso match"; },
        { "target": "test_dot_1"; "expected_output": 30; "description": "Accesso attributo annidato"; },
        { "target": "test_dot_2"; "expected_output": 10; "description": "Chiamata metodo su dizionario"; },
        { "target": "check_include_var"; "expected_output": "PRESENTE"; "description": "Variabile da include"; },
        { "target": "check_include_func"; "expected_output": 10; "description": "Funzione da include"; },
        { "target": "p1"; "expected_output": 100; "description": "Mappa tra parentesi"; }
    ];
}
