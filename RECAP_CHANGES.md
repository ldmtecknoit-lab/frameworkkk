# Recap Modifiche: DSL Contract System

Questo documento riassume le modifiche effettuate per implementare il sistema di contratti basato su DSL e risolvere i problemi di bootstrap.

## 1. Upgrade del Sistema di Contratti
- **DSL come Sorgente di Verità**: Il framework ora utilizza i file `.test.dsl` per definire le esportazioni pubblicate e i test di validazione.
- **Generazione Automatica**: Eseguendo `python3 public/main.py --test-save`, il sistema:
    1. Carica il modulo Python.
    2. Esegue i test definiti nel DSL.
    3. Se i test passano, genera un file `.contract.json` contenente gli hash del codice sorgente e dei test.

## 2. Risoluzione dei Problemi di Bootstrap
- **Self-Hosting del Loader**: È stato risolto l'errore `AttributeError: module 'filtered:framework/service/load.py' has no attribute 'resource'`.
- **Force-Exposure**: In `load.py`, i metodi core (`resource`, `bootstrap`, `register`, `generate_checksum`) vengono ora forzatamente esposti anche se il contratto non è ancora stato validato. Questo previene il deadlock durante l'inizializzazione del framework.
- **Gestione Circolare**: Migliorata la gestione delle dipendenze circolari nel caricamento dei moduli filtrati.

## 3. Supporto per Funzioni Importate
- **Introspezione AST**: Modificato `inspector.py` per tracciare `ast.Import` e `ast.ImportFrom`.
- **Hash di Alias**: È ora possibile mettere sotto contratto funzioni che sono semplicemente importate da altri moduli (es. `get`, `put`, `convert` in `language.py`). L'hash viene calcolato sulla riga di importazione, garantendo che la "promessa" di esportazione sia mantenuta.

## 4. Refactoring di `load.py`
- **ValidationContext**: Introdotta una struttura dati per gestire lo stato della validazione di ogni modulo.
- **ContractEngine**: Centralizzata la logica di caricamento dei contratti, risoluzione delle esportazioni e verifica degli hash.
- **ModuleBuilder**: Separata la logica di creazione dei proxy (moduli filtrati) che nascondono le funzioni non validate o non testate.

## 5. Correzione Test DSL
- Aggiornati i file `.test.dsl` (`load`, `flow`, `language`, `run`) per utilizzare la sintassi corretta e mappare correttamente le funzioni esportate.

## Comandi Utili
- **Regenerazione Contratti**: `python3 public/main.py --test-save`
- **Esecuzione Normale**: `python3 public/main.py`
