{
    # Configurazione globale
    configuration : "pyproject.toml" | resource | format | convert(dict, "toml");
    ports : ("presentation", "persistence", "message", "authentication", "actuator","authorization");

    # Generazione dinamica dei servizi basata sulle porte
    mask : { 
        "path": "infrastructure/{key}/{val.backend.adapter}.py"; 
        "service": "@.key"; 
        "adapter": "adapter";
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
        {"path": "framework/manager/tester.py"; "service": "tester"; "config": {"cache_enabled": True; "log_level": "INFO";}; "dependency_keys": ("messenger","persistence"); "manager": "tester"; },
        {"path": "framework/manager/messenger.py"; "service": "messenger"; "config": {"cache_enabled": True; "log_level": "INFO";}; "dependency_keys": ("message"); "manager": "messenger"; },
        {"path": "framework/manager/executor.py"; "service": "executor"; "config": {"cache_enabled": True; "log_level": "INFO";}; "dependency_keys": ("actuator"); "manager": "executor"; },
        {"path": "framework/manager/presenter.py"; "service": "presenter"; "config": {"cache_enabled": True; "log_level": "INFO";}; "dependency_keys": ("messenger"); "manager": "presenter"; },
        {"path": "framework/manager/defender.py"; "service": "defender"; "config": {"cache_enabled": True; "log_level": "INFO";}; "dependency_keys": ("authentication"); "manager": "defender"; },
        {"path": "framework/manager/storekeeper.py"; "service": "storekeeper"; "config": {"cache_enabled": True; "log_level": "INFO";}; "dependency_keys": ("persistence"); "manager": "storekeeper"; }
    );
    
    # Registrazione dei servizi (Infrastruttura) e poi dei manager
    services_list : static_services | merge(dynamic_services);
    
    registered_services : services_list | foreach(register);
    registered_managers : managers | foreach(register);

    # Listener degli eventi
    messenger.read(domain:'ciao'): messenger.post(message:"Hello World")|print;
    59,*,*,*,*,: "ciao cronos" | print;
    *,*,*,*,*,: messenger.post(domain:'ciao', message:'Triggering Event') | print;
}