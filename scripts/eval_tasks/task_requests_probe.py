#!/usr/bin/env python3
"""Probe de avaliação: usa `requests` (catálogo → python:3.11-slim) sem rede externa."""
import requests

def main():
    # Só precisa da lib instalada na imagem; não depende de internet.
    print("requests_version", requests.__version__)
    print("ok")

if __name__ == "__main__":
    main()
