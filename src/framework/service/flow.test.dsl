imports: {
    'contract': 'framework/service/contract.py';
};

exports: {
    'asynchronous': 'asynchronous';
    'synchronous': 'synchronous';
    'format': 'format';
    'transform': 'transform';
    'convert': 'convert';
    'route': 'route';
    'normalize': 'normalize';
    'put': 'put';
    'get': 'get';
    'work': 'work';
    'step': 'step';
    'pipe': 'pipe';
    'catch': 'catch';
};

str:match_score_label :=
    75 |> match({
        "@ > 90": "Ottimo";
        "@ > 60": "Sufficiente";
        "*": "Insufficiente";
    });

function:attivazione := (int:x),{
    f:x |> match({
        "@ >= 50": "Attivo";
        "@ < 50": "Inattivo";
    });
},(int:f);

list:score_list := (85,75,65,55,45,35,25,15,5,0) |> foreach(attivazione);

any:module := resource("framework/service/flow.py");

#any:catch_error := module.catch(attivazione,print,{x:10;}) |> print;

any:switch_1 := 12 |> module._dsl_switch({
        "@ > 90": "Ottimo";
        "@ <= 60": "Sufficiente";
        "": "Insufficiente";
    }) |> print;

tuple:test_suite := (
    { "target": "match_score_label"; "output": "Sufficiente"; "description": "Match flow"; },
    { "target": "score_list"; "output": ["Attivo", "Attivo", "Attivo", "Attivo", "Inattivo", "Inattivo", "Inattivo", "Inattivo", "Inattivo", "Inattivo"]; "description": "Match flow list"; },
    
);