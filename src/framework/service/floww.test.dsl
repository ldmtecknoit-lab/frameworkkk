imports: {
    'flow':resource("framework/service/floww.py");
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
    'catch':  imports.flow.catch;
};


any:catch_error := exports.catch(print,print,{inputs:[10];}) |> print;

tuple:test_suite := (
    { "target": "match_score_label"; "output": "Sufficiente"; "description": "Match flow"; },
    { "target": "score_list"; "output": ["Attivo", "Attivo", "Attivo", "Attivo", "Inattivo", "Inattivo", "Inattivo", "Inattivo", "Inattivo", "Inattivo"]; "description": "Match flow list"; },
);