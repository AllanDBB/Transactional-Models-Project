from supabase import create_client
from dotenv import load_dotenv
import os

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://vzcwfryxmtzocmjpayfz.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

print("Consultando órdenes existentes...")
result = supabase.table("orden").select("*").limit(5).execute()
if result.data:
    print(f"Encontradas {len(result.data)} órdenes:")
    for orden in result.data:
        print(f"  Canal: {orden.get('canal')}, Moneda: {orden.get('moneda')}")
else:
    print("No hay órdenes existentes")

print("\nIntentando consultar el schema...")
# Intentar insertar con todos los posibles valores comunes
test_canales = ["Web", "Móvil", "Tienda", "Online", "Teléfono", "App", "POS", "web", "movil", "tienda", "online", "telefono", "app", "pos"]
print(f"Probando canales: {test_canales[:5]}...")
