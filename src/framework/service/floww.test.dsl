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
    'serial': imports.flow.serial;
    'parallel': imports.flow.parallel;
    'retry': imports.flow.retry;
    'pipeline': imports.flow.pipeline;
    'sentry': imports.flow.sentry;
    'switch': imports.flow.switch;
    'when': imports.flow.when;
};

type:scheme := {
    "action": {
        "type": "string";
        "default": "unknown";
    };
    "inputs": {
        "type": "list";
        "default": [];
    };
    "outputs": {
        "type": "list";
        "default": [];
        "_convert": list;
    };
    "errors": {
        "type": "list";
        "default": [];
    };
    "success": {
        "type": "boolean";
        "default": false;
    };
    "time": {
        "type": "string";
        "default": "0";
    };
    "worker": {
        "type": "string";
        "default": "unknown";
    };
};

function:error_function := (str:y),{
    x:y/2;
},(str:x);

any:catch_error := exports.catch(error_function,print,{inputs:["test"];}) |> print;

any:foreach_test := exports.serial([1,2,3],print,{inputs:["test"];}) |> print;

any:parallel_test := exports.parallel(print,print,context:{inputs:["test"];}) |> print;

any:pipeline_test := exports.pipeline(print,print,context:{inputs:["test"];}) |> print;

any:retry_test := exports.retry(error_function,context:{inputs:["test"];}) |> print;

any:sentry_test := exports.sentry("True",context:{inputs:["test"];}) |> print;

any:switch_test := exports.switch({"True": print; "1 == 2": print;},context:{inputs:["test"];}) |> print;

scheme:when_test_success := exports.when("1 == 1", print,context:{inputs:["test"];});
scheme:when_test_failure := exports.when("1 == 2", print,context:{inputs:["test"];});

tuple:test_suite := (
    { "target": "match_score_label"; "output": "Sufficiente"; "description": "Match flow"; },
    { "target": "score_list"; "output": ["Attivo", "Attivo", "Attivo", "Attivo", "Inattivo", "Inattivo", "Inattivo", "Inattivo", "Inattivo", "Inattivo"]; "description": "Match flow list"; },

);