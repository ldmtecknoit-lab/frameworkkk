{
    # Configurazione globale (Dichiarazioni con := e tipi)
    dict:configuration := "pyproject.toml" |> resource |> format |> convert(dict:dict, "toml");
    
    # Per tuple di stringhe, usiamo le parentesi tonde esplicite (mapping)
    ports : ("presentation", "persistence", "message", "authentication", "actuator", "authorization");

    # Generazione dinamica dei servizi (Definizione con :=)
    service:mask := { 
        "path": "infrastructure/{key}/{val.backend.adapter}.py"; 
        "service": "{key}"; 
        "adapter": "adapter";
        "payload": "{val}";
    };

    function:add := (int:x, int:y), { int:z: x + y; }, (int:z);
    
    # Utilizzo (Mapping con : e nomi semplici)
    services:services := configuration
        |> filter(ports)
        |> items
        |> remap("key", "val")
        |> transform(mask);

    boolean:stato := 1 == "presentation" | Vero;

    managers:managers := (
        {"path": "framework/manager/tester.py"; "service": "tester"; "config": {"cache_enabled": Vero; "log_level": "INFO";}; "dependency_keys": ("messenger","persistence"); "manager": "tester"; },
        {"path": "framework/manager/messenger.py"; "service": "messenger"; "config": {"cache_enabled": Vero; "log_level": "INFO";}; "dependency_keys": ("message"); "manager": "messenger"; },
        {"path": "framework/manager/executor.py"; "service": "executor"; "config": {"cache_enabled": Vero; "log_level": "INFO";}; "dependency_keys": ("actuator"); "manager": "executor"; },
        {"path": "framework/manager/presenter.py"; "service": "presenter"; "config": {"cache_enabled": Vero; "log_level": "INFO";}; "dependency_keys": ("messenger"); "manager": "presenter"; },
        {"path": "framework/manager/defender.py"; "service": "defender"; "config": {"cache_enabled": Vero; "log_level": "INFO";}; "dependency_keys": ("authentication"); "manager": "defender"; },
        {"path": "framework/manager/storekeeper.py"; "service": "storekeeper"; "config": {"cache_enabled": Vero; "log_level": "INFO";}; "dependency_keys": ("persistence"); "manager": "storekeeper"; }
    );
    
    # Priorità degli operatori garantita dalla nuova grammatica (Pipe > Boolean)
    success : (managers |> foreach(register)) & (services |> foreach(register));

    messenger.read(domain:'info'): messenger.post(message:"Hello World") |> print;
    crono(59,*,*,*,*): "ciao ogni 1 minuto" |> print;

    # Test function add
    test_result : add(10, 20) |> print;

    int:numero := "ciao";
}