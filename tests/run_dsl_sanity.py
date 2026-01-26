import asyncio
import sys
import os

# Aggiungiamo src al path per importare il framework
sys.path.append(os.path.join(os.getcwd(), 'src'))

from framework.service.language import parse_dsl_file, run_dsl_tests, DSLVisitor, dsl_functions

async def main():
    print("🧪 Avvio DSL Sanity Check...\n")
    
    path = "tests/dsl_sanity_check.dsl"
    if not os.path.exists(path):
        print(f"❌ File {path} non trovato.")
        return

    with open(path, 'r') as f:
        content = f.read()

    try:
        # 1. Parsing
        print("🔍 Step 1: Parsing e Trasformazione...")
        parsed_data = parse_dsl_file(content)
        print(f"DEBUG: Parsed Data Keys: {list(parsed_data.keys()) if isinstance(parsed_data, dict) else type(parsed_data)}")
        
        # 2. Esecuzione Test Suite
        print("🏃 Step 2: Esecuzione Test Suite interna...")
        visitor = DSLVisitor(dsl_functions)
        
        # Popoliamo il visitatore con i dati iniziali (le definizioni nel file)
        # Il metodo run() esegue tutto il file e restituisce il contesto finale
        context = await visitor.run(parsed_data)
        
        # Eseguiamo la validazione formale
        success = await run_dsl_tests(visitor, context)
        
        if success:
            print("\n✨ TUTTI I TEST DSL SONO PASSATI!")
            sys.exit(0)
        else:
            print("\n❌ ALCUNI TEST DSL SONO FALLITI.")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n💥 ERRORE CRITICO durante l'esecuzione: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
