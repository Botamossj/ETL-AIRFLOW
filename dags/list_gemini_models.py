#!/usr/bin/env python3
"""
Script para listar todos los modelos de Gemini disponibles.
"""

import os
import google.generativeai as genai

# API Key
API_KEY = os.environ.get("LLM_API_KEY", "AIzaSyCXagg3BcPlPc_v_wWh6yG1vjKEzaWxEuM")

print("=" * 60)
print("LISTANDO MODELOS DISPONIBLES DE GEMINI")
print("=" * 60)

genai.configure(api_key=API_KEY)

try:
    # Listar todos los modelos disponibles
    models = genai.list_models()
    
    modelos_list = list(models)
    print(f"\n✅ Modelos disponibles ({len(modelos_list)}):\n")
    
    modelos_pro = []
    modelos_flash = []
    otros_modelos = []
    
    for model in modelos_list:
        model_name = model.name.replace('models/', '')
        supported_methods = list(model.supported_generation_methods) if hasattr(model, 'supported_generation_methods') else []
        
        if 'generateContent' in supported_methods:
            if 'pro' in model_name.lower():
                modelos_pro.append((model_name, supported_methods))
            elif 'flash' in model_name.lower():
                modelos_flash.append((model_name, supported_methods))
            else:
                otros_modelos.append((model_name, supported_methods))
    
    if modelos_pro:
        print("📊 MODELOS PRO:")
        for name, methods in modelos_pro:
            print(f"   ✅ {name}")
            print(f"      Métodos: {', '.join(methods)}")
    
    if modelos_flash:
        print("\n⚡ MODELOS FLASH:")
        for name, methods in modelos_flash:
            print(f"   ✅ {name}")
            print(f"      Métodos: {', '.join(methods)}")
    
    if otros_modelos:
        print("\n📦 OTROS MODELOS:")
        for name, methods in otros_modelos:
            print(f"   ✅ {name}")
            print(f"      Métodos: {', '.join(methods)}")
    
    # Recomendación
    if modelos_pro:
        print(f"\n🎯 RECOMENDACIÓN: Usar '{modelos_pro[0][0]}'")
    elif modelos_flash:
        print(f"\n🎯 RECOMENDACIÓN: Usar '{modelos_flash[0][0]}'")
    
except Exception as e:
    print(f"\n❌ Error al listar modelos: {e}")
    import traceback
    traceback.print_exc()

