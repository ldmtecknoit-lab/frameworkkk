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

test_suite: (
    { target: 'get'; input_args: (data, "nome"); expected_output: "Progetto A"; },
    { target: 'get'; input_args: (data, "config.timeout"); expected_output: 30; },
    { target: 'get'; input_args: (data, "versioni.0.status"); expected_output: "completo"; },
    { target: 'get'; input_args: (data, "versioni.1.dettagli.tester"); expected_output: "Mario"; },
    { target: 'get'; input_args: (data, "versioni.*.status"); expected_output: ("completo", "in_corso", "fallito"); },
    { target: 'get'; input_args: (data, "versioni.*.id"); expected_output: (1, 2, 3); }
);