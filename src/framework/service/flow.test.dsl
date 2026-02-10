imports: {
    'flow':resource("framework/service/floww.py");
};

exports: {
    'assert': imports.flow.assertt;
    'foreach': imports.flow.foreach;
    'pass': imports.flow.passs;
    'catch':  imports.flow.catch;
    'serial': imports.flow.serial;
    'parallel': imports.flow.parallel;
    'retry': imports.flow.retry;
    'pipeline': imports.flow.pipeline;
    'sentry': imports.flow.sentry;
    'switch': imports.flow.switch;
    'when': imports.flow.when;
    'timeout': imports.flow.timeout;
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
        "convert": list;
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

scheme:catch_error := exports.catch(error_function,print,{inputs:["test"];}) |> print;

scheme:foreach_test := exports.serial([1,2,3],print,{inputs:["test"];}) |> print;

scheme:parallel_test := exports.parallel(print,print,context:{inputs:["test"];}) |> print;

scheme:pipeline_test := exports.pipeline(print,print,context:{inputs:["test"];}) |> print;

scheme:retry_test := exports.retry(error_function,context:{inputs:["test"];}) |> print;

scheme:sentry_test := exports.sentry("True",context:{inputs:["test"];}) |> print;

scheme:switch_test := exports.switch({"True": print; "1 == 2": print;},context:{inputs:["test"];}) |> print;

scheme:when_test_success := exports.when("1 == 1", print,context:{inputs:["test"];});
scheme:when_test_failure := exports.when("1 == 2", print,context:{inputs:["test"];});

#scheme:test_assert_failure := exports.assert("10 >= 50");
#scheme:test_assert_success := exports.assert("10 <= 50");

any:pass_test := exports.pass(10);

tuple:test_suite := (
    { "target": "pass_test"; "output": pass_test |> put("outputs",10); "description": "Pass flow"; },
    { "target": "when_test_success"; "output": when_test_success |> put("outputs",["test"]); "description": "Match flow"; },
    { "target": "when_test_failure"; "output": when_test_failure |> put("outputs",[]); "description": "Match flow"; },
    #{ "target": "test_assert_failure"; "output": test_assert_failure |> put("outputs",[]); "description": "Match flow"; },
    #{ "target": "test_assert_success"; "output": test_assert_success |> put("outputs",["10 <= 50"]); "description": "Match flow"; },

);