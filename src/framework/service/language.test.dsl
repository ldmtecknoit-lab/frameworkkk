{
    # ============================================================
    # 1. TIPI BASE
    # ============================================================

    int:const_int := 10;
    float:const_float := 20.5;
    str:const_str := "Ciao";
    str:const_str_alt := 'Mondo';

    boolean:bool_true := True;
    boolean:bool_false := False;

    any:any_str := "Ciao";
    any:any_int := 10;
    any:any_bool := True;
    any:any_list := [1, 2, 3];
    any:any_dict := { "a": 1; "b": 2; };
    any:any_tuple := (1, 2, 3);
    any:any_fn := (int:x), { r: x + 1; }, (int:r);


    # ============================================================
    # 2. ESPRESSIONI MATEMATICHE
    # ============================================================

    int:calc_precedence := 2 + 3 * 4;       # 14
    int:calc_grouped := (2 + 3) * 4;        # 20
    int:calc_div_sub := 10 / 2 - 1;          # 4
    int:calc_mod := 10 % 3;                  # 1
    int:calc_power := 2 ^ 3;                 # 8


    # ============================================================
    # 3. LOGICA E COMPARAZIONI
    # ============================================================

    boolean:logic_and_or :=
        True & (10 > 5) or False;

    boolean:logic_not :=
        not (5 == 5) or (1 != 2);

    boolean:logic_comparison_chain :=
        (10 >= 10) & (5 <= 6) & (2 < 3) & (4 > 1);


    # ============================================================
    # 4. COLLEZIONI
    # ============================================================

    list:collection_mixed_list := [1, 2, "tre", Vero];
    dict:collection_simple_dict := { "chiave": "valore"; "num": 42; };

    tuple:collection_pair := (1, "test");
    tuple:collection_inline_tuple := 1, 2, 3;

    list:collection_list_trailing := [1, 2];
    dict:collection_dict_trailing := { "a": 1; "b": 2; };


    # ============================================================
    # 5. FUNZIONI
    # ============================================================

    function:fn_double :=
        (int:n),
        {
            out: n * 2;
        },
        (int:out);

    function:fn_sum :=
        (int:x, int:y),
        {
            sum: x + y;
        },
        (int:sum);

    function:fn_increment_pair :=
        (int:val),
        {
            inc1: val + 1;
            inc2: val + 2;
        },
        (int:inc1, int:inc2);


    # ============================================================
    # 6. RISULTATI FUNZIONI
    # ============================================================

    int:res_double := fn_double(5);                    # 10
    int:res_sum := fn_sum(10, 20);                     # 30
    tuple:res_increment_pair := fn_increment_pair(100);# (101, 102)

    int:res_sum_kw := fn_sum(y: 50, x: 10);            # 60


    # ============================================================
    # 7. PIPE
    # ============================================================

    int:pipe_chain_double :=
        10 |> fn_double |> fn_double;                  # 40

    int:pipe_partial_sum :=
        10 |> fn_sum(5);                               # 15


    # ============================================================
    # 8. LIBRERIA STANDARD
    # ============================================================

    dict:lib_merged_dict :=
        merge({ "a": 1; }, { "b": 2; });

    list:lib_concatenated_list :=
        concat([1], [2]);

    dict:lib_selected_keys :=
        { "a": 1; "b": 2; "c": 3; } |> pick(["a", "c"]);

    list:lib_dict_keys :=
        keys({ "x": 1; "y": 2; });

    list:lib_dict_values :=
        values({ "x": 1; "y": 2; });

    str:lib_formatted :=
        format("Hello {{name}}", name: "World");

    list:lib_mapped :=
        [1, 2, 3] |> map("@ * 2");

    dict:lib_projected :=
        { "nested": { "val": 99; }; }
        |> project({ "out": "@.nested.val"; });

    int:lib_query_result :=
        { "data": [10, 20, 30]; } |> query("data[1]");

    # ============================================================
    # 10. DOT NOTATION / OGGETTI
    # ============================================================

    dict:service := {
        "config": { "timeout": 30; };
        "action": (int:x), { r: x + 1; }, (int:r);
    };

    int:res_service_timeout := service.config.timeout;  # 30
    int:res_service_action := service.action(9);        # 10


    # ============================================================
    # 11. EDGE CASE
    # ============================================================

    (mapped_value: 100);


    # ============================================================
    # 12. TEST SUITE
    # ============================================================

    list:test_suite := [
        { "target": "const_int"; "output": 10; "description": "Costante int"; },
        { "target": "const_str"; "output": "Ciao"; "description": "Costante str"; },
        { "target": "bool_true"; "output": True; "description": "Costante bool"; },
        
        { "target": "any_int"; "output": 10; "description": "Any int"; },
        { "target": "any_str"; "output": "Ciao"; "description": "Any str"; },
        { "target": "any_bool"; "output": True; "description": "Any bool"; },
        { "target": "any_list"; "output": [1, 2, 3]; "description": "Any list"; },
        { "target": "any_dict"; "output": { "a": 1; "b": 2; }; "description": "Any dict"; },
        { "target": "any_tuple"; "output": (1, 2, 3); "description": "Any tuple"; },

        { "target": "calc_precedence"; "output": 14; "description": "Precedenza operatori"; },
        { "target": "calc_grouped"; "output": 20; "description": "Parentesi esplicite"; },
        { "target": "calc_power"; "output": 8; "description": "Operatore potenza"; },

        { "target": "logic_and_or"; "output": True; "description": "Logica AND/OR"; },
        { "target": "logic_not"; "output": True; "description": "Operatore NOT"; },

        { "target": "collection_pair"; "output": (1, "test"); "description": "Tuple standard"; },
        { "target": "collection_inline_tuple"; "output": (1, 2, 3); "description": "Tuple inline"; },

        { "target": "res_double"; "output": 10; "description": "Funzione double"; },
        { "target": "res_increment_pair"; "output": (101, 102); "description": "Ritorno multiplo"; },
        { "target": "res_sum_kw"; "output": 60; "description": "Keyword arguments"; },

        { "target": "pipe_chain_double"; "output": 40; "description": "Pipe chaining"; },
        { "target": "pipe_partial_sum"; "output": 15; "description": "Pipe con argomenti"; },

        { "target": "lib_merged_dict"; "output": { "a": 1; "b": 2; }; "description": "Merge dict"; },
        { "target": "lib_selected_keys"; "output": { "a": 1; "c": 3; }; "description": "Pick keys"; },
        { "target": "lib_dict_keys"; "output": ["x", "y"]; "description": "Keys"; },
        { "target": "lib_formatted"; "output": "Hello World"; "description": "Format string"; },
        { "target": "lib_mapped"; "output": [2.0, 4.0, 6.0]; "description": "Map"; },
        { "target": "lib_query_result"; "output": 20.0; "description": "Query"; },

        { "target": "res_service_timeout"; "output": 30; "description": "Dot access"; },
        { "target": "res_service_action"; "output": 10; "description": "Metodo su dict"; },

        { "target": "mapped_value"; "output": 100; "description": "Mapping tra parentesi"; }
    ];
}