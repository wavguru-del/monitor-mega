#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MEGALEILÕES - SCRIPT DE MONITORAMENTO
✅ Scrape de todas as páginas
✅ Match com links no schema 'auctions'
✅ Detecção de mudanças e snapshots
"""

import os
import time
import re
from datetime import datetime, timezone
from typing import List, Dict, Optional
from zoneinfo import ZoneInfo

from supabase import create_client, Client
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

def convert_brazilian_datetime_to_postgres(date_str: str) -> Optional[str]:
    """Converte data brasileira DD/MM/YYYY HH:MM para PostgreSQL ISO format"""
    try:
        date_str = date_str.replace('às', '').strip()
        dt = datetime.strptime(date_str, '%d/%m/%Y %H:%M')
        dt_with_tz = dt.replace(tzinfo=ZoneInfo('America/Sao_Paulo'))
        return dt_with_tz.isoformat()
    except Exception:
        return None

class MegaLeiloesMonitor:
    def __init__(self):
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        self.schema = "auctions"  # <--- Definido o schema correto
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("❌ Variáveis SUPABASE_URL e SUPABASE_KEY são obrigatórias")
        
        # Inicializa o cliente Supabase apontando explicitamente para o schema 'auctions'
        self.supabase: Client = create_client(
            self.supabase_url, 
            self.supabase_key,
            options={"schema": self.schema}
        )
        
        self.source = 'megaleiloes'
        self.base_url = 'https://www.megaleiloes.com.br'
        
        self.sections = [
            ('imoveis', 'Imóveis'),
            ('veiculos', 'Veículos'),
            ('bens-de-consumo', 'Bens de Consumo'),
            ('industrial', 'Industrial'),
            ('animais', 'Animais'),
            ('outros', 'Outros'),
        ]
        
        self.stats = {
            'items_scraped': 0, 'items_matched': 0, 'items_new': 0,
            'snapshots_created': 0, 'items_updated': 0, 'bid_changes': 0,
            'value_changes': 0, 'status_changes': 0, 'pages_scraped': 0, 'errors': 0,
        }
        
        self.db_items_by_link = {}
        self.db_items_by_id = {}
        self.last_snapshots = {}

    def run(self):
        print("\n" + "="*70)
        print(f"🔍 MEGALEILÕES - MONITORAMENTO (Schema: {self.schema})")
        print("="*70)
        
        start_time = time.time()
        
        # 1. Carrega dados
        print("\n📊 Carregando itens da base de dados...")
        self._load_database_items()
        
        print("\n📸 Carregando últimos snapshots...")
        self._load_last_snapshots()
        
        # 2. Scrape
        print("\n🌐 Iniciando scrape completo...")
        scraped_data = self._scrape_all_sections()
        
        # 3. Processamento
        print("\n🔄 Processando matches e mudanças...")
        self._process_matches_and_snapshots(scraped_data)
        
        elapsed = time.time() - start_time
        self._print_stats(elapsed)

    def _load_database_items(self):
        """Busca itens na tabela megaleiloes_items dentro do schema auctions"""
        try:
            # O cliente já está no schema auctions, então chamamos a tabela normalmente
            response = self.supabase.table('megaleiloes_items') \
                .select('*') \
                .eq('source', self.source) \
                .execute()
            
            for item in response.data:
                link = item.get('link', '').split('?')[0].rstrip('/')
                self.db_items_by_link[link] = item
                self.db_items_by_id[item['id']] = item
            
            print(f"✅ {len(self.db_items_by_link)} itens carregados.")
        except Exception as e:
            print(f"❌ Erro ao carregar itens: {e}")
            raise

    def _load_last_snapshots(self):
        """Carrega últimos snapshots do schema auctions"""
        try:
            if not self.db_items_by_id: return
            
            item_ids = list(self.db_items_by_id.keys())
            for i in range(0, len(item_ids), 1000):
                batch = item_ids[i:i+1000]
                response = self.supabase.table('megaleiloes_monitoring') \
                    .select('*') \
                    .in_('item_id', batch) \
                    .order('snapshot_at', desc=True) \
                    .execute()
                
                seen = set()
                for snap in response.data:
                    item_id = snap['item_id']
                    if item_id not in seen:
                        self.last_snapshots[item_id] = snap
                        seen.add(item_id)
        except Exception as e:
            print(f"⚠️ Erro ao carregar snapshots: {e}")

    def _scrape_all_sections(self) -> List[Dict]:
        all_items = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent="Mozilla/5.0", viewport={'width': 1920, 'height': 1080})
            page = context.new_page()
            
            for url_path, display_name in self.sections:
                print(f"📦 {display_name}...")
                section_items = self._scrape_section(page, url_path)
                all_items.extend(section_items)
                time.sleep(1)
            browser.close()
        self.stats['items_scraped'] = len(all_items)
        return all_items

    def _scrape_section(self, page, url_path: str) -> List[Dict]:
        items = []
        url = f"{self.base_url}/{url_path}"
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            time.sleep(2)
            soup = BeautifulSoup(page.content(), 'html.parser')
            max_page = self._get_max_page(soup)
            
            for p_num in range(1, max_page + 1):
                if p_num > 1:
                    page.goto(f"{url}?pagina={p_num}", wait_until="domcontentloaded")
                    soup = BeautifulSoup(page.content(), 'html.parser')
                
                cards = soup.select('div.card')
                for card in cards:
                    parsed = self._parse_card(card)
                    if parsed: items.append(parsed)
                self.stats['pages_scraped'] += 1
        except Exception as e:
            print(f"❌ Erro na seção {url_path}: {e}")
        return items

    def _get_max_page(self, soup) -> int:
        try:
            last_link = soup.select_one('ul.pagination li.last a')
            if last_link:
                match = re.search(r'pagina=(\d+)', last_link.get('href', ''))
                return int(match.group(1))
            return 1
        except: return 1

    def _parse_card(self, card) -> Optional[Dict]:
        try:
            link_elem = card.select_one('a[href]')
            if not link_elem: return None
            link = link_elem.get('href', '')
            if not link.startswith('http'): link = f"{self.base_url}{link}"
            link_clean = link.split('?')[0].rstrip('/')
            
            info = self._extract_auction_info(card)
            
            # Detecção simples de atividade
            texto = card.get_text().lower()
            is_active = not ('encerrado' in texto or 'finalizado' in texto)
            
            # Lances
            has_bid = False
            legal_icon = card.select_one('i.fa-legal')
            if legal_icon:
                text = legal_icon.find_parent('span').get_text()
                nums = re.findall(r'\d+', text)
                has_bid = int(nums[0]) > 0 if nums else False

            return {
                'link': link_clean,
                'value': info['current_value'],
                'has_bid': has_bid,
                'auction_round': info['auction_round'],
                'auction_date': info['auction_date'],
                'first_round_value': info['first_round_value'],
                'first_round_date': info['first_round_date'],
                'discount_percentage': info['discount_percentage'],
                'is_active': is_active
            }
        except: return None

    def _extract_auction_info(self, card) -> Dict:
        res = {'auction_round': None, 'auction_date': None, 'current_value': None, 
               'first_round_value': None, 'first_round_date': None, 'discount_percentage': None}
        
        # Instance Ativa
        active = card.select_one('.instance.active')
        if active:
            val_txt = active.select_one('.card-instance-value')
            if val_txt:
                match = re.search(r'R\$\s*([\d.]+,\d{2})', val_txt.get_text())
                if match: res['current_value'] = float(match.group(1).replace('.','').replace(',','.'))
            
            date_elem = active.select_one('.card-second-instance-date, .card-first-instance-date')
            if date_elem:
                res['auction_round'] = 2 if 'second' in str(date_elem) else 1
                dt_match = re.search(r'(\d{2}/\d{2}/\d{4})\s*às\s*(\d{2}:\d{2})', date_elem.get_text())
                if dt_match: res['auction_date'] = convert_brazilian_datetime_to_postgres(f"{dt_match.group(1)} {dt_match.group(2)}")

        # Instance Passada (para pegar valor da 1ª praça se estivermos na 2ª)
        first_passed = card.select_one('.instance.first.passed')
        if first_passed:
            val_txt = first_passed.select_one('.card-instance-value')
            if val_txt:
                match = re.search(r'R\$\s*([\d.]+,\d{2})', val_txt.get_text())
                if match: res['first_round_value'] = float(match.group(1).replace('.','').replace(',','.'))
        
        if res['first_round_value'] and res['current_value'] and res['current_value'] < res['first_round_value']:
            res['discount_percentage'] = round((1 - (res['current_value'] / res['first_round_value'])) * 100, 2)
            
        return res

    def _process_matches_and_snapshots(self, scraped_data: List[Dict]):
        snaps, updates = [], []
        
        for s_item in scraped_data:
            db_item = self.db_items_by_link.get(s_item['link'])
            if not db_item:
                self.stats['items_new'] += 1
                continue
            
            self.stats['items_matched'] += 1
            last_snap = self.last_snapshots.get(db_item['id'])
            
            # Lógica de snapshot
            snap = self._create_snapshot_obj(db_item, s_item, last_snap)
            if snap: snaps.append(snap)
            
            # Lógica de update da tabela principal
            updates.append(self._create_update_obj(db_item['id'], s_item))

        if snaps: self._insert_snapshots_batch(snaps)
        if updates: self._update_base_items_batch(updates)

    def _create_snapshot_obj(self, db_item, s_item, last_snap):
        # Simplificado: detecta se houve mudança relevante para criar snapshot
        curr_val = s_item['value']
        prev_val = last_snap['current_value'] if last_snap else db_item.get('value')
        
        has_bid_now = s_item['has_bid']
        has_bid_before = last_snap['has_bid'] if last_snap else db_item.get('has_bid', False)

        # Só gera estatísticas se houver algo para comparar
        value_change = (curr_val - prev_val) if curr_val and prev_val else 0
        
        if value_change != 0: self.stats['value_changes'] += 1
        if has_bid_now != has_bid_before: self.stats['bid_changes'] += 1

        return {
            'item_id': db_item['id'],
            'external_id': db_item['external_id'],
            'snapshot_at': datetime.now(timezone.utc).isoformat(),
            'current_value': curr_val,
            'value_change': value_change,
            'has_bid': has_bid_now,
            'is_active': s_item['is_active'],
            'auction_round': s_item['auction_round'],
            'bid_status_changed': has_bid_now != has_bid_before,
            'status_changed': s_item['is_active'] != db_item.get('is_active')
        }

    def _create_update_obj(self, item_id, s_item):
        return {
            'id': item_id,
            **s_item,
            'updated_at': datetime.now(timezone.utc).isoformat(),
            'last_scraped_at': datetime.now(timezone.utc).isoformat()
        }

    def _insert_snapshots_batch(self, snapshots: List[Dict]):
        try:
            self.supabase.table('megaleiloes_monitoring').insert(snapshots).execute()
            self.stats['snapshots_created'] = len(snapshots)
        except Exception as e: print(f"❌ Erro Snapshots: {e}")

    def _update_base_items_batch(self, updates: List[Dict]):
        # Supabase Python não suporta bulk update via .update() em lista facilmente 
        # (exige loop ou RPC). Usaremos loop para garantir integridade.
        for up in updates:
            try:
                self.supabase.table('megaleiloes_items').update(up).eq('id', up['id']).execute()
                self.stats['items_updated'] += 1
            except: self.stats['errors'] += 1

    def _print_stats(self, elapsed: float):
        print(f"\n📊 FINALIZADO EM {int(elapsed)}s")
        print(f"• Scraped: {self.stats['items_scraped']} | Matched: {self.stats['items_matched']}")
        print(f"• Snapshots: {self.stats['snapshots_created']} | Updates: {self.stats['items_updated']}")
        print(f"• Mudanças - Lances: {self.stats['bid_changes']} | Valor: {self.stats['value_changes']}")

if __name__ == "__main__":
    MegaLeiloesMonitor().run()