import asyncio
import sys
import os

# Aggiungiamo src al path per importare il framework
sys.path.append(os.path.join(os.getcwd(), 'src'))

from framework.service.language import parse_dsl_file

content = """
{
    int:costante_venti := 20;
    function:somma_dieci := (int:x), { r: x + 10; }, (int:r);
    int:test_pipe := costante_venti |> somma_dieci;
}
"""

try:
    parsed_data = parse_dsl_file(content)
    print(f"Parsed Data Type: {type(parsed_data)}")
    for k, v in parsed_data.items():
        print(f"Key: {k} | Value: {v}")
except Exception as e:
    print(f"Error: {e}")
