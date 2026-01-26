import asyncio
import sys
import os

# Aggiungiamo src al path per importare il framework
sys.path.append(os.path.join(os.getcwd(), 'src'))

from framework.service.language import parse_dsl_file

content = """
{
    boolean:test_logic := Vero & (1 == 1);
}
"""

try:
    parsed_data = parse_dsl_file(content)
    print(f"Parsed Data Type: {type(parsed_data)}")
    for k, v in parsed_data.items():
        print(f"Key: {k} | Value: {v}")
except Exception as e:
    import traceback
    traceback.print_exc()
