{
    # Configurazione globale
    configuration : "pyproject.toml" | resource | format | convert(dict, "toml");
    ports : ("presentation", "persistence", "message", "authentication", "actuator","authorization");

    # Generazione dinamica dei servizi basata sulle porte
    mask : { 
        "path": "infrastructure/{key}/{val.backend.adapter}.py"; 
        "service": "@.key"; 
        "adapter": "@.val.backend.adapter"; 
        "payload": "@.val"; 
    };
    
    dynamic_services : configuration 
        | filter(ports)
        | items
        | remap("key", "val")
        | transform(mask);

    static_services : (
        {"path": "infrastructure/message/console.py"; "service": "message"; "adapter": "adapter"; "payload": configuration;}
    );

    managers : (
        {"path": "framework/manager/messenger.py"; "service": "messenger"; "config": {"cache_enabled": True; "log_level": "INFO";}; "dependency_keys": ("message"); "messenger": "messenger"; },
        {"path": "framework/manager/executor.py"; "service": "executor"; "config": {"cache_enabled": True; "log_level": "INFO";}; "dependency_keys": ("actuator"); "messenger": "executor"; },
        {"path": "framework/manager/presenter.py"; "service": "presenter"; "config": {"cache_enabled": True; "log_level": "INFO";}; "dependency_keys": ("messenger"); "messenger": "presenter"; },
        {"path": "framework/manager/defender.py"; "service": "defender"; "config": {"cache_enabled": True; "log_level": "INFO";}; "dependency_keys": ("authentication"); "messenger": "defender"; },
        {"path": "framework/manager/storekeeper.py"; "service": "storekeeper"; "config": {"cache_enabled": True; "log_level": "INFO";}; "dependency_keys": ("persistence"); "messenger": "storekeeper"; },
        {"path": "framework/manager/tester.py"; "service": "tester"; "config": {"cache_enabled": True; "log_level": "INFO";}; "dependency_keys": ("messenger","persistence"); "messenger": "tester"; }
    );
    
    registered_managers : managers | foreach(register) | print;

    ok: messenger().post(message:"Hello World")|print;
    
    services : static_services | merge(dynamic_services) | foreach(register) | print;
}