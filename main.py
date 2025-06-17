#!/usr/bin/env python3
"""
Gong Webhook System - Main Entry Point
Sistema completo para procesar transcripts de Gong automáticamente.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.orchestrator import main

if __name__ == "__main__":
    main()