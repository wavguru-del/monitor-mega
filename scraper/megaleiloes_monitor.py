#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MEGALEILÕES - SCRIPT DE MONITORAMENTO
✅ Scrape todas as páginas normalmente (como o scraper)
✅ Faz match com links da tabela base
✅ Detecta mudanças (lances, valores, status)
✅ Insere snapshots na tabela de monitoramento
✅ Atualiza tabela base com dados frescos
"""

import os
import time
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import List, Dict, Optional
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
    """Monitor para MegaLeilões - scrape completo e match com base"""
    
    def __init__(self):
        """Inicializa monitor"""
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("❌ Variáveis SUPABASE_URL e SUPABASE_KEY são obrigatórias")
        
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
        self.source = 'megaleiloes'
        self.base_url = 'https://www.megaleiloes.com.br'
        
        # Seções para scrape
        self.sections = [
            ('imoveis', 'Imóveis'),
            ('veiculos', 'Veículos'),
            ('bens-de-consumo', 'Bens de Consumo'),
            ('industrial', 'Industrial'),
            ('animais', 'Animais'),
            ('outros', 'Outros'),
        ]
        
        self.stats = {
            'items_scraped': 0,
            'items_matched': 0,
            'items_new': 0,
            'snapshots_created': 0,
            'items_updated': 0,
            'bid_changes': 0,
            'value_changes': 0,
            'status_changes': 0,
            'pages_scraped': 0,
            'errors': 0,
        }
        
        # Cache da base de dados (link -> dados do item)
        self.db_items_by_link = {}
        self.db_items_by_id = {}
        self.last_snapshots = {}
    
    def run(self):
        """Executa monitoramento completo"""
        print("\n" + "="*70)
        print("🔍 MEGALEILÕES - MONITORAMENTO")
        print("="*70)
        print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        
        start_time = time.time()
        
        # 1. Carrega dados da base em memória
        print("\n📊 Carregando itens da base de dados...")
        self._load_database_items()
        print(f"✅ {len(self.db_items_by_link)} itens carregados da base")
        
        # 2. Carrega últimos snapshots
        print("\n📸 Carregando últimos snapshots...")
        self._load_last_snapshots()
        print(f"✅ {len(self.last_snapshots)} snapshots anteriores carregados")
        
        # 3. Scrape de todas as páginas
        print("\n🌐 Iniciando scrape completo...")
        scraped_data = self._scrape_all_sections()
        print(f"✅ {len(scraped_data)} itens scrapados")
        
        # 4. Processa matches e gera snapshots
        print("\n🔄 Processando matches e mudanças...")
        self._process_matches_and_snapshots(scraped_data)
        
        # 5. Estatísticas finais
        elapsed = time.time() - start_time
        self._print_stats(elapsed)
    
    def _load_database_items(self):
        """Carrega todos os itens ativos da base em memória"""
        try:
            response = self.supabase.table('auctions.megaleiloes_items') \
                .select('*') \
                .eq('source', 'megaleiloes') \
                .execute()
            
            if response.data:
                for item in response.data:
                    # Normaliza o link (remove params UTM e trailing slash)
                    link = item.get('link', '').split('?')[0].rstrip('/')
                    self.db_items_by_link[link] = item
                    self.db_items_by_id[item['id']] = item
            
        except Exception as e:
            print(f"❌ Erro ao carregar itens da base: {e}")
            raise
    
    def _load_last_snapshots(self):
        """Carrega último snapshot de cada item"""
        try:
            if not self.db_items_by_id:
                return
            
            item_ids = list(self.db_items_by_id.keys())
            
            # Busca em lotes de 1000
            for i in range(0, len(item_ids), 1000):
                batch = item_ids[i:i+1000]
                
                response = self.supabase.table('megaleiloes_monitoring') \
                    .select('*') \
                    .in_('item_id', batch) \
                    .order('snapshot_at', desc=True) \
                    .execute()
                
                if response.data:
                    # Pega apenas o mais recente de cada item
                    seen = set()
                    for snap in response.data:
                        item_id = snap['item_id']
                        if item_id not in seen:
                            self.last_snapshots[item_id] = snap
                            seen.add(item_id)
        
        except Exception as e:
            print(f"⚠️ Erro ao carregar snapshots: {e}")
    
    def _scrape_all_sections(self) -> List[Dict]:
        """Scrape todas as seções - igual ao scraper original"""
        all_items = []
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
                
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    viewport={'width': 1920, 'height': 1080},
                    locale='pt-BR'
                )
                
                page = context.new_page()
                
                for url_path, display_name in self.sections:
                    print(f"\n{'='*70}")
                    print(f"📦 {display_name}")
                    print(f"{'='*70}")
                    
                    section_items = self._scrape_section(page, url_path, display_name)
                    all_items.extend(section_items)
                    
                    print(f"✅ {len(section_items)} itens coletados de {display_name}")
                    time.sleep(2)
                
                browser.close()
        
        except Exception as e:
            print(f"❌ Erro no scrape: {e}")
            import traceback
            traceback.print_exc()
        
        self.stats['items_scraped'] = len(all_items)
        return all_items
    
    def _scrape_section(self, page, url_path: str, display_name: str) -> List[Dict]:
        """Scrape uma seção - todas as páginas"""
        items = []
        url = f"{self.base_url}/{url_path}"
        
        try:
            # Primeira página
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            time.sleep(3)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)
            
            html = page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            # Detecta número de páginas
            max_page = self._get_max_page(soup)
            print(f"📄 Total de páginas detectadas: {max_page}")
            
            # Scrape todas as páginas
            for page_num in range(1, max_page + 1):
                if page_num == 1:
                    current_soup = soup
                else:
                    current_url = f"{url}?pagina={page_num}"
                    page.goto(current_url, wait_until="domcontentloaded", timeout=60000)
                    time.sleep(3)
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    time.sleep(2)
                    current_html = page.content()
                    current_soup = BeautifulSoup(current_html, 'html.parser')
                
                # Extrai cards
                cards = current_soup.select('div.card')
                
                if not cards:
                    print(f"  ⚠️ Página {page_num}/{max_page}: Nenhum card encontrado")
                    continue
                
                print(f"  📄 Página {page_num}/{max_page}: {len(cards)} cards encontrados")
                
                page_items = 0
                for card in cards:
                    item = self._parse_card(card)
                    if item:
                        items.append(item)
                        page_items += 1
                
                self.stats['pages_scraped'] += 1
                print(f"  ✅ {page_items} itens extraídos da página {page_num}")
                time.sleep(2)
        
        except Exception as e:
            print(f"❌ Erro ao processar seção: {e}")
            import traceback
            traceback.print_exc()
        
        return items
    
    def _get_max_page(self, soup) -> int:
        """Detecta número máximo de páginas"""
        try:
            last_link = soup.select_one('ul.pagination li.last a')
            if last_link:
                href = last_link.get('href', '')
                match = re.search(r'pagina=(\d+)', href)
                if match:
                    return int(match.group(1))
            
            page_links = soup.select('ul.pagination li a[data-page]')
            if page_links:
                pages = []
                for link in page_links:
                    href = link.get('href', '')
                    match = re.search(r'pagina=(\d+)', href)
                    if match:
                        pages.append(int(match.group(1)))
                if pages:
                    return max(pages)
            
            return 1
        except Exception:
            return 1
    
    def _parse_card(self, card) -> Optional[Dict]:
        """Parse de um card - versão simplificada focada em dados de monitoramento"""
        try:
            # 1. Link (obrigatório para match)
            link_elem = card.select_one('a[href]')
            if not link_elem:
                return None
            
            link = link_elem.get('href', '')
            if not link or 'javascript' in link.lower():
                return None
            
            if not link.startswith('http'):
                link = f"{self.base_url}{link}"
            
            link_clean = link.split('?')[0].rstrip('/')
            
            # 2. Extrai informações de praça
            auction_info = self._extract_auction_info_from_html(card)
            
            # 3. Has bid
            has_bid = self._extract_has_bid(card)
            
            # 4. Valor
            value = auction_info.get('current_value')
            
            # 5. Is active (se tem data de leilão no futuro ou está "aberto para lances")
            is_active = True
            texto = card.get_text(separator=' ', strip=True).lower()
            if 'encerrado' in texto or 'finalizado' in texto:
                is_active = False
            
            return {
                'link': link_clean,
                'value': value,
                'has_bid': has_bid,
                'auction_round': auction_info.get('auction_round'),
                'auction_date': auction_info.get('auction_date'),
                'first_round_value': auction_info.get('first_round_value'),
                'first_round_date': auction_info.get('first_round_date'),
                'discount_percentage': auction_info.get('discount_percentage'),
                'is_active': is_active,
            }
        
        except Exception:
            return None
    
    def _extract_has_bid(self, card) -> bool:
        """Verifica se tem lances"""
        try:
            legal_icon = card.select_one('i.fa-legal')
            if legal_icon:
                parent_span = legal_icon.find_parent('span')
                if parent_span:
                    text = parent_span.get_text(strip=True)
                    numbers = re.findall(r'\d+', text)
                    if numbers:
                        return int(numbers[0]) > 0
            return False
        except Exception:
            return False
    
    def _extract_auction_info_from_html(self, card) -> Dict:
        """Extrai informações de praça"""
        info = {
            'auction_round': None,
            'auction_date': None,
            'current_value': None,
            'first_round_value': None,
            'first_round_date': None,
            'discount_percentage': None,
        }
        
        active_instance = card.select_one('.instance.active')
        
        if active_instance:
            second_date = active_instance.select_one('.card-second-instance-date')
            first_date = active_instance.select_one('.card-first-instance-date')
            
            if second_date:
                info['auction_round'] = 2
                date_text = second_date.get_text(strip=True)
                date_match = re.search(r'(\d{2}/\d{2}/\d{4})\s*às\s*(\d{2}:\d{2})', date_text)
                if date_match:
                    date_str = f"{date_match.group(1)} {date_match.group(2)}"
                    info['auction_date'] = convert_brazilian_datetime_to_postgres(date_str)
            elif first_date:
                info['auction_round'] = 1
                date_text = first_date.get_text(strip=True)
                date_match = re.search(r'(\d{2}/\d{2}/\d{4})\s*às\s*(\d{2}:\d{2})', date_text)
                if date_match:
                    date_str = f"{date_match.group(1)} {date_match.group(2)}"
                    info['auction_date'] = convert_brazilian_datetime_to_postgres(date_str)
            
            value_elem = active_instance.select_one('.card-instance-value')
            if value_elem:
                value_text = value_elem.get_text(strip=True)
                value_match = re.search(r'R\$\s*([\d.]+,\d{2})', value_text)
                if value_match:
                    try:
                        info['current_value'] = float(value_match.group(1).replace('.', '').replace(',', '.'))
                    except:
                        pass
        
        first_instance = card.select_one('.instance.first.passed')
        if first_instance:
            date_elem = first_instance.select_one('.card-first-instance-date')
            if date_elem:
                date_text = date_elem.get_text(strip=True)
                date_match = re.search(r'(\d{2}/\d{2}/\d{4})\s*às\s*(\d{2}:\d{2})', date_text)
                if date_match:
                    date_str = f"{date_match.group(1)} {date_match.group(2)}"
                    info['first_round_date'] = convert_brazilian_datetime_to_postgres(date_str)
            
            value_elem = first_instance.select_one('.card-instance-value')
            if value_elem:
                value_text = value_elem.get_text(strip=True)
                value_match = re.search(r'R\$\s*([\d.]+,\d{2})', value_text)
                if value_match:
                    try:
                        info['first_round_value'] = float(value_match.group(1).replace('.', '').replace(',', '.'))
                    except:
                        pass
        
        if info['first_round_value'] and info['current_value'] and info['auction_round'] == 2:
            try:
                discount = ((info['first_round_value'] - info['current_value']) / info['first_round_value']) * 100
                info['discount_percentage'] = round(discount, 2)
            except:
                pass
        
        return info
    
    def _process_matches_and_snapshots(self, scraped_data: List[Dict]):
        """Processa matches com base e gera snapshots"""
        snapshots_batch = []
        updates_batch = []
        
        for scraped_item in scraped_data:
            link = scraped_item['link']
            
            # Verifica se existe na base
            db_item = self.db_items_by_link.get(link)
            
            if not db_item:
                self.stats['items_new'] += 1
                continue
            
            self.stats['items_matched'] += 1
            
            # Busca último snapshot
            last_snap = self.last_snapshots.get(db_item['id'])
            
            # Calcula mudanças
            snapshot = self._create_snapshot(db_item, scraped_item, last_snap)
            
            if snapshot:
                snapshots_batch.append(snapshot)
                self.stats['snapshots_created'] += 1
                
                # Detecta mudanças para stats
                if snapshot['bid_status_changed']:
                    self.stats['bid_changes'] += 1
                if snapshot.get('value_change') and abs(snapshot['value_change']) > 0:
                    self.stats['value_changes'] += 1
                if snapshot['status_changed']:
                    self.stats['status_changes'] += 1
            
            # Prepara update da tabela base
            update = self._create_update(db_item, scraped_item)
            if update:
                updates_batch.append(update)
        
        # Insere snapshots em lotes
        if snapshots_batch:
            print(f"\n💾 Inserindo {len(snapshots_batch)} snapshots...")
            self._insert_snapshots_batch(snapshots_batch)
        
        # Atualiza tabela base em lotes
        if updates_batch:
            print(f"\n🔄 Atualizando {len(updates_batch)} itens na tabela base...")
            self._update_base_items_batch(updates_batch)
    
    def _create_snapshot(self, db_item: Dict, scraped_item: Dict, last_snap: Optional[Dict]) -> Optional[Dict]:
        """Cria snapshot de monitoramento"""
        try:
            # Calcula days_until_auction
            days_until_auction = None
            if scraped_item.get('auction_date'):
                try:
                    auction_dt = datetime.fromisoformat(scraped_item['auction_date'])
                    now = datetime.now(timezone.utc)
                    delta = auction_dt - now
                    days_until_auction = delta.days
                except:
                    pass
            
            # Calcula mudanças de valor
            old_value = last_snap['current_value'] if last_snap else db_item.get('value')
            new_value = scraped_item.get('value')
            
            value_change = None
            value_change_percentage = None
            if old_value and new_value:
                value_change = new_value - old_value
                if old_value > 0:
                    value_change_percentage = (value_change / old_value) * 100
            
            # Calcula desconto da primeira rodada
            discount_from_first_round = None
            if scraped_item.get('first_round_value') and new_value:
                first_val = scraped_item['first_round_value']
                if first_val > 0:
                    discount_from_first_round = ((first_val - new_value) / first_val) * 100
            
            # Detecta mudanças
            old_has_bid = last_snap['has_bid'] if last_snap else db_item.get('has_bid', False)
            bid_status_changed = scraped_item.get('has_bid') != old_has_bid
            
            old_round = last_snap['auction_round'] if last_snap else db_item.get('auction_round')
            round_changed = scraped_item.get('auction_round') != old_round
            
            old_auction_date = last_snap['auction_date'] if last_snap else db_item.get('auction_date')
            auction_date_changed = scraped_item.get('auction_date') != old_auction_date
            
            old_is_active = last_snap['is_active'] if last_snap else db_item.get('is_active', True)
            status_changed = scraped_item.get('is_active') != old_is_active
            
            # Calcula hours_since_last_snapshot
            hours_since_last = None
            if last_snap and last_snap.get('snapshot_at'):
                try:
                    last_time = datetime.fromisoformat(last_snap['snapshot_at'])
                    now = datetime.now(timezone.utc)
                    delta = now - last_time
                    hours_since_last = delta.total_seconds() / 3600
                except:
                    pass
            
            # Calcula value_velocity
            value_velocity = None
            if value_change and hours_since_last and hours_since_last > 0:
                value_velocity = value_change / hours_since_last
            
            snapshot = {
                'item_id': db_item['id'],
                'external_id': db_item['external_id'],
                'snapshot_at': datetime.now(timezone.utc).isoformat(),
                'days_until_auction': days_until_auction,
                'current_value': new_value,
                'value_change': value_change,
                'value_change_percentage': value_change_percentage,
                'first_round_value': scraped_item.get('first_round_value'),
                'discount_from_first_round': discount_from_first_round,
                'has_bid': scraped_item.get('has_bid', False),
                'bid_status_changed': bid_status_changed,
                'auction_round': scraped_item.get('auction_round'),
                'round_changed': round_changed,
                'auction_date': scraped_item.get('auction_date'),
                'auction_date_changed': auction_date_changed,
                'is_active': scraped_item.get('is_active', True),
                'status_changed': status_changed,
                'category': db_item.get('category'),
                'city': db_item.get('city'),
                'state': db_item.get('state'),
                'auction_type': db_item.get('auction_type'),
                'hours_since_last_snapshot': hours_since_last,
                'value_velocity': value_velocity,
                'metadata': {'source': 'automated_monitoring'}
            }
            
            return snapshot
        
        except Exception as e:
            self.stats['errors'] += 1
            print(f"⚠️ Erro ao criar snapshot: {e}")
            return None
    
    def _create_update(self, db_item: Dict, scraped_item: Dict) -> Optional[Dict]:
        """Cria update para tabela base"""
        try:
            update = {
                'id': db_item['id'],
                'value': scraped_item.get('value'),
                'has_bid': scraped_item.get('has_bid'),
                'auction_round': scraped_item.get('auction_round'),
                'auction_date': scraped_item.get('auction_date'),
                'first_round_value': scraped_item.get('first_round_value'),
                'first_round_date': scraped_item.get('first_round_date'),
                'discount_percentage': scraped_item.get('discount_percentage'),
                'is_active': scraped_item.get('is_active'),
                'updated_at': datetime.now(timezone.utc).isoformat(),
                'last_scraped_at': datetime.now(timezone.utc).isoformat(),
            }
            
            return update
        
        except Exception as e:
            self.stats['errors'] += 1
            return None
    
    def _insert_snapshots_batch(self, snapshots: List[Dict]):
        """Insere snapshots em lotes"""
        try:
            batch_size = 500
            for i in range(0, len(snapshots), batch_size):
                batch = snapshots[i:i+batch_size]
                self.supabase.table('megaleiloes_monitoring').insert(batch).execute()
                print(f"  ✅ Lote {i//batch_size + 1}: {len(batch)} snapshots inseridos")
        
        except Exception as e:
            print(f"❌ Erro ao inserir snapshots: {e}")
            self.stats['errors'] += len(snapshots)
    
    def _update_base_items_batch(self, updates: List[Dict]):
        """Atualiza tabela base em lotes"""
        try:
            for update in updates:
                try:
                    self.supabase.table('auctions.megaleiloes_items') \
                        .update(update) \
                        .eq('id', update['id']) \
                        .execute()
                    self.stats['items_updated'] += 1
                except Exception as e:
                    print(f"  ⚠️ Erro ao atualizar item {update['id']}: {e}")
                    self.stats['errors'] += 1
            
            print(f"  ✅ {self.stats['items_updated']} itens atualizados")
        
        except Exception as e:
            print(f"❌ Erro ao atualizar itens: {e}")
    
    def _print_stats(self, elapsed: float):
        """Imprime estatísticas finais"""
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        
        print(f"\n{'='*70}")
        print("📊 ESTATÍSTICAS FINAIS")
        print(f"{'='*70}")
        print(f"\n  Scrape:")
        print(f"    • Páginas processadas: {self.stats['pages_scraped']}")
        print(f"    • Itens scrapados: {self.stats['items_scraped']}")
        print(f"\n  Match:")
        print(f"    • Itens encontrados na base: {self.stats['items_matched']}")
        print(f"    • Itens novos (não na base): {self.stats['items_new']}")
        print(f"\n  Monitoramento:")
        print(f"    • Snapshots criados: {self.stats['snapshots_created']}")
        print(f"    • Itens atualizados: {self.stats['items_updated']}")
        print(f"\n  Mudanças detectadas:")
        print(f"    • Mudanças de lances: {self.stats['bid_changes']}")
        print(f"    • Mudanças de valor: {self.stats['value_changes']}")
        print(f"    • Mudanças de status: {self.stats['status_changes']}")
        
        if self.stats['errors'] > 0:
            print(f"\n  ⚠️ Erros: {self.stats['errors']}")
        
        print(f"\n⏱️ Duração: {minutes}min {seconds}s")
        print(f"✅ Concluído: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*70}")


def main():
    """Execução principal"""
    try:
        monitor = MegaLeiloesMonitor()
        monitor.run()
    
    except Exception as e:
        print(f"\n❌ Erro fatal: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()