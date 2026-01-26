{
    # 1. Definizioni di base
    int:costante_venti := 20;
    str:saluto := "Ciao";
    
    function:moltiplica := (int:a, int:b), { 
        r: a * b; 
    }, (int:r);
    
    function:somma_dieci := (int:x), { 
        r: x + 10; 
    }, (int:r);

    # 2. Test Suite (Dichiarazione tipizzata come lista)
    list:test_suite := [
        { "target": "matematica"; "expected_output": 14; "description": "Precedenza standard: 2 + 3 * 4 = 14"; },
        { "target": "test_pipe"; "expected_output": 30; "description": "Pipe: 20 |> somma_dieci = 30"; },
        { "target": "test_logic"; "expected_output": Vero; "description": "Logica: Vero & (1 == 1)"; }
    ];

    # Implementazioni per i target dei test
    int:matematica := 2 + 3 * 4;
    int:test_pipe := costante_venti |> somma_dieci;
    boolean:test_logic := Vero & (1 == 1);
}
