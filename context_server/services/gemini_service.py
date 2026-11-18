"""
Serviço de Integração com Google Gemini API
Responsável por determinar a imagem Docker mais adequada baseada no código da tarefa.
"""

import json
import os
from typing import Optional

import google.generativeai as genai

from context_server import config
from context_server.logging_config import get_logger

logger = get_logger(__name__)


def get_docker_image_from_code(script_content: str) -> Optional[str]:
    """
    Analisa o código da tarefa e retorna a imagem Docker mais otimizada.
    
    Args:
        script_content: Conteúdo completo do script da tarefa (string)
    
    Returns:
        Nome da imagem Docker (ex: 'python:3.11-slim') ou None em caso de erro
    """
    response_text = None
    try:
        # Configura o cliente do Gemini
        genai.configure(api_key=config.GOOGLE_GEMINI_API_KEY)
        
        # Modelo a ser usado
        model = genai.GenerativeModel('gemini-2.5-pro')
        
        # Log detalhado do código que será enviado
        logger.info("=" * 80)
        logger.info("📄 DEBUG: Código da Tarefa a ser enviado ao Gemini")
        logger.info("=" * 80)
        logger.info(f"📊 Tamanho do código: {len(script_content)} caracteres")
        logger.info(f"📊 Número de linhas: {len(script_content.splitlines())} linhas")
        logger.info(f"🔤 Primeiros 500 caracteres do código:\n{script_content[:500]}")
        logger.info(f"🔤 Últimos 500 caracteres do código:\n{script_content[-500:]}")
        logger.info("=" * 80)
        logger.info("📝 CÓDIGO COMPLETO DA TAREFA:")
        logger.info("=" * 80)
        logger.info(script_content)
        logger.info("=" * 80)
        
        # Prompt com chave adicional temporária para forçar geração completa do JSON
        prompt = f"""Você é um Serviço de Mapeamento de Imagem Docker. Analise o código da tarefa fornecido (que pode estar em qualquer linguagem: Python, Node.js, R, Java, etc.) e determine a **Imagem Docker MAIS OTIMIZADA** e **MAIS ESPECÍFICA** para a sua execução.

A imagem escolhida deve ter o formato de download padrão (repositório:tag, ex: 'python:3.11-slim', 'node:20', 'tensorflow/tensorflow:latest').

Você DEVE retornar o resultado como um objeto JSON estrito com as seguintes chaves:
- 'docker_image': o nome da imagem Docker escolhida
- 'justificativa': uma breve justificativa (máximo 20 palavras) sobre por que essa imagem foi escolhida

Retorne APENAS o JSON, sem nenhuma explicação ou texto adicional antes ou depois.

### CÓDIGO A SER ANALISADO:

{script_content}
"""
        
        logger.info(f"📊 Tamanho do prompt completo: {len(prompt)} caracteres")
        
        # Configuração da requisição
        generation_config = genai.types.GenerationConfig(
            temperature=0.1,  # Baixa temperatura para respostas mais determinísticas
            max_output_tokens=300,  # Aumentado para acomodar a justificativa temporária
        )
        
        # Log completo do corpo da requisição
        logger.info("=" * 80)
        logger.info("📤 DEBUG: Corpo completo da requisição enviada à API do Gemini")
        logger.info("=" * 80)
        logger.info(f"🤖 Modelo: gemini-2.5-pro")
        logger.info(f"🌡️ Temperature: {generation_config.temperature}")
        logger.info(f"📏 Max Output Tokens: {generation_config.max_output_tokens}")
        logger.info("=" * 80)
        logger.info("📝 PROMPT COMPLETO (corpo da requisição):")
        logger.info("=" * 80)
        logger.info(prompt)
        logger.info("=" * 80)
        logger.info(f"📊 Tamanho do prompt: {len(prompt)} caracteres")
        logger.info("=" * 80)
        
        logger.debug("Enviando código para análise do Gemini...")
        
        # Gera a resposta do Gemini
        response = model.generate_content(
            prompt,
            generation_config=generation_config
        )
        
        # Log detalhado do objeto response completo
        logger.info("=" * 80)
        logger.info("🔍 DEBUG: Resposta completa do Gemini")
        logger.info("=" * 80)
        logger.info(f"Tipo do objeto response: {type(response)}")
        logger.info(f"Atributos do response: {dir(response)}")
        
        # Tenta acessar diferentes propriedades da resposta
        try:
            if hasattr(response, 'text'):
                response_text = response.text.strip()
                logger.info(f"📝 response.text (completo):\n{response_text}")
                logger.info(f"📏 Tamanho da resposta: {len(response_text)} caracteres")
            else:
                logger.warning("⚠️ Objeto response não possui atributo 'text'")
                logger.info(f"📦 Conteúdo do response: {response}")
                response_text = str(response).strip()
        except Exception as e:
            logger.error(f"❌ Erro ao acessar response.text: {e}")
            logger.info(f"📦 Tentando converter response para string: {str(response)}")
            response_text = str(response).strip()
        
        # Log da resposta bruta
        logger.debug(f"Resposta bruta do Gemini (response_text): {response_text}")
        logger.info(f"🔤 Primeiros 200 caracteres: {response_text[:200]}")
        logger.info(f"🔤 Últimos 200 caracteres: {response_text[-200:]}")
        
        # Tenta extrair JSON da resposta
        # O Gemini pode retornar JSON dentro de markdown code blocks ou diretamente
        json_text = response_text
        logger.info(f"📋 json_text inicial (antes do processamento):\n{json_text}")
        
        # Remove markdown code blocks se existirem
        if "```json" in json_text:
            logger.debug("Encontrado bloco ```json, extraindo...")
            json_text = json_text.split("```json")[1].split("```")[0].strip()
            logger.info(f"📋 json_text após remover ```json:\n{json_text}")
        elif "```" in json_text:
            logger.debug("Encontrado bloco ```, extraindo...")
            json_text = json_text.split("```")[1].split("```")[0].strip()
            logger.info(f"📋 json_text após remover ```:\n{json_text}")
        else:
            logger.debug("Nenhum bloco de código encontrado, usando resposta direta")
        
        logger.info(f"📋 json_text final (que será parseado):\n{json_text}")
        logger.info(f"📏 Tamanho do json_text: {len(json_text)} caracteres")
        logger.info("=" * 80)
        
        # Parse do JSON
        result = json.loads(json_text)
        
        # Extrai apenas a chave 'docker_image' (ignoramos a 'justificativa' temporária)
        docker_image = result.get('docker_image')
        
        if not docker_image:
            logger.error("Resposta do Gemini não contém a chave 'docker_image'")
            logger.error(f"Chaves disponíveis no JSON: {list(result.keys())}")
            return None
        
        # Log da justificativa (se presente) para debug
        if 'justificativa' in result:
            logger.debug(f"Justificativa do Gemini: {result.get('justificativa')}")
        
        logger.info(f"✅ Imagem Docker determinada pelo Gemini: {docker_image}")
        return docker_image
        
    except json.JSONDecodeError as e:
        logger.error("=" * 80)
        logger.error(f"❌ ERRO ao fazer parse do JSON retornado pelo Gemini: {e}")
        logger.error("=" * 80)
        if response_text:
            logger.error(f"📝 Resposta completa que causou erro:")
            logger.error(f"{response_text}")
            logger.error(f"📏 Tamanho: {len(response_text)} caracteres")
            logger.error(f"🔤 Primeiros 500 caracteres: {response_text[:500]}")
            logger.error(f"🔤 Últimos 500 caracteres: {response_text[-500:]}")
            # Log de cada caractere para debug de caracteres especiais
            logger.error(f"🔍 Primeiros 50 caracteres (repr): {repr(response_text[:50])}")
            logger.error(f"🔍 Últimos 50 caracteres (repr): {repr(response_text[-50:])}")
        else:
            logger.error("⚠️ response_text está None ou vazio")
        logger.error("=" * 80)
        return None
    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"❌ Erro ao consultar API do Gemini: {e}")
        logger.error(f"📋 Tipo do erro: {type(e).__name__}")
        import traceback
        logger.error(f"📋 Traceback completo:\n{traceback.format_exc()}")
        if response_text:
            logger.error(f"📝 Resposta disponível: {response_text[:200]}...")
        logger.error("=" * 80)
        return None

