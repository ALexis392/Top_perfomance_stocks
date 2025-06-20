import os
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils.dataframe import dataframe_to_rows
warnings.filterwarnings('ignore')

class DataFetcher:
    """Módulo para obtener datos históricos de acciones y ETFs"""
    
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
    
    def fetch_stock_data(self, symbol):
        """Obtiene datos históricos para un símbolo específico"""
        try:
            stock = yf.Ticker(symbol)
            data = stock.history(start=self.start_date, end=self.end_date)
            if data.empty:
                print(f"Sin datos para {symbol}")
                return None
            return data['Close']
        except Exception as e:
            print(f"Error obteniendo {symbol}: {e}")
            return None
    
    def fetch_multiple_stocks(self, symbols, batch_size=100):
        """Obtiene datos para múltiples símbolos de forma eficiente"""
        print(f"Descargando datos para {len(symbols)} símbolos desde {self.start_date} hasta {self.end_date}...")
        
        results = {}
        total_batches = (len(symbols) + batch_size - 1) // batch_size
        
        for i in range(0, len(symbols), batch_size):
            batch_symbols = symbols[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            print(f"Procesando lote {batch_num}/{total_batches} ({len(batch_symbols)} símbolos)")
            
            try:
                # Filtrar símbolos válidos
                valid_symbols = [s for s in batch_symbols if isinstance(s, str) and len(s) > 0]
                
                if not valid_symbols:
                    continue
                
                symbols_str = " ".join(valid_symbols)
                data = yf.download(symbols_str, start=self.start_date, end=self.end_date, 
                                 group_by='ticker', progress=False, threads=True)
                
                for symbol in valid_symbols:
                    try:
                        if len(valid_symbols) == 1:
                            stock_data = data['Close'] if 'Close' in data.columns else data
                        else:
                            if symbol in data.columns.get_level_values(0):
                                stock_data = data[symbol]['Close']
                            else:
                                print(f"No se encontraron datos para {symbol}")
                                continue
                        
                        if not stock_data.empty and not stock_data.isna().all():
                            # Verificar que tengamos al menos 1 año de datos
                            if len(stock_data.dropna()) > 50:  # ~50 días de trading por año mínimo
                                results[symbol] = stock_data.dropna()
                            else:
                                print(f"Datos insuficientes para {symbol}")
                        else:
                            print(f"Datos vacíos para {symbol}")
                            
                    except Exception as e:
                        print(f"Error procesando {symbol}: {e}")
                        continue
                        
            except Exception as e:
                print(f"Error en lote {batch_num}: {e}")
                continue
        
        print(f"✅ Datos obtenidos exitosamente para {len(results)} de {len(symbols)} símbolos")
        return results

class PerformanceCalculator:
    """Módulo para calcular métricas de rendimiento"""
    
    @staticmethod
    def calculate_annual_returns(price_data, start_year, end_year):
        """Calcula rendimientos anuales a partir de datos de precios"""
        if price_data is None or price_data.empty:
            return {}
        
        annual_returns = {}
        price_data = price_data.dropna()
        
        for year in range(start_year, end_year + 1):
            try:
                # Obtener datos del año
                year_data = price_data[price_data.index.year == year]
                
                if len(year_data) < 10:  # Mínimo 10 observaciones por año
                    continue
                
                # Precio al inicio y final del año
                start_price = year_data.iloc[0]
                end_price = year_data.iloc[-1]
                
                if start_price > 0:
                    annual_return = (end_price / start_price - 1) * 100
                    annual_returns[year] = annual_return
                    
            except Exception as e:
                print(f"Error calculando rendimiento para {year}: {e}")
                continue
        
        return annual_returns
    
    @staticmethod
    def calculate_total_return(price_data):
        """Calcula el rendimiento total del período"""
        if price_data is None or len(price_data) < 2:
            return 0
        
        try:
            start_price = price_data.iloc[0]
            end_price = price_data.iloc[-1]
            
            if start_price > 0:
                return (end_price / start_price - 1) * 100
            return 0
        except:
            return 0
    
    @staticmethod
    def calculate_cagr(price_data, years):
        """Calcula la tasa de crecimiento anual compuesta (CAGR)"""
        if price_data is None or len(price_data) < 2 or years <= 0:
            return 0
        
        try:
            start_price = price_data.iloc[0]
            end_price = price_data.iloc[-1]
            
            if start_price > 0:
                return (((end_price / start_price) ** (1/years)) - 1) * 100
            return 0
        except:
            return 0
    
    @staticmethod
    def calculate_sharpe_ratio(returns, risk_free_rate=0.02):
        """Calcula el ratio de Sharpe"""
        if not returns or len(returns) < 2:
            return 0
        
        try:
            returns_array = np.array(list(returns.values()))
            excess_returns = returns_array - risk_free_rate * 100
            
            if np.std(returns_array) == 0:
                return 0
            
            return np.mean(excess_returns) / np.std(returns_array)
        except:
            return 0
    
    @staticmethod
    def calculate_max_drawdown(price_data):
        """Calcula el drawdown máximo"""
        if price_data is None or len(price_data) < 2:
            return 0
        
        try:
            cumulative = (1 + price_data.pct_change().fillna(0)).cumprod()
            running_max = cumulative.expanding().max()
            drawdown = (cumulative - running_max) / running_max
            
            return abs(drawdown.min()) * 100
        except:
            return 0
    
    @staticmethod
    def calculate_volatility(price_data):
        """Calcula la volatilidad anualizada"""
        if price_data is None or len(price_data) < 2:
            return 0
        
        try:
            returns = price_data.pct_change().dropna()
            if len(returns) == 0:
                return 0
            return returns.std() * np.sqrt(252) * 100  # Anualizada
        except:
            return 0

class BenchmarkComparator:
    """Módulo para comparar rendimiento contra benchmarks"""
    
    def __init__(self, benchmark_returns):
        self.benchmark_returns = benchmark_returns
    
    def compare_performance(self, stock_returns, benchmark_name):
        """Compara rendimiento de una acción contra un benchmark"""
        if not stock_returns or benchmark_name not in self.benchmark_returns:
            return {}
        
        comparison = {}
        benchmark_data = self.benchmark_returns[benchmark_name]
        
        for year in stock_returns:
            if year in benchmark_data:
                stock_ret = stock_returns[year]
                bench_ret = benchmark_data[year]
                
                comparison[year] = {
                    'stock_return': stock_ret,
                    'benchmark_return': bench_ret,
                    'outperformance': stock_ret - bench_ret,
                    'beats_benchmark': stock_ret > bench_ret
                }
        
        return comparison
    
    def count_outperformance_years(self, stock_returns, benchmark_name):
        """Cuenta años de outperformance contra un benchmark"""
        comparison = self.compare_performance(stock_returns, benchmark_name)
        
        if not comparison:
            return 0, 0, 0
        
        total_years = len(comparison)
        outperform_years = sum(1 for data in comparison.values() if data['beats_benchmark'])
        avg_outperformance = np.mean([data['outperformance'] for data in comparison.values()])
        
        return outperform_years, total_years, avg_outperformance

class OutperformanceFilter:
    """Módulo para filtrar acciones con outperformance sostenida"""
    
    def __init__(self, comparator, min_years_percentage=0.8):
        self.comparator = comparator
        self.min_years_percentage = min_years_percentage
    
    def filter_sustained_outperformers(self, stock_returns_dict, benchmarks, total_years):
        """Filtra acciones que superan sostenidamente a todos los benchmarks"""
        min_years = max(1, int(total_years * self.min_years_percentage))
        sustained_outperformers = []
        
        print(f"Filtros: Mínimo {min_years} de {total_years} años para cada benchmark")
        
        for symbol, returns in stock_returns_dict.items():
            if not returns:
                continue
            
            beats_all_benchmarks = True
            performance_summary = {'symbol': symbol}
            
            for benchmark in benchmarks:
                outperform_years, stock_total_years, avg_outperformance = \
                    self.comparator.count_outperformance_years(returns, benchmark)
                
                performance_summary[f'beats_{benchmark}_years'] = outperform_years
                performance_summary[f'total_years'] = stock_total_years
                performance_summary[f'{benchmark}_avg_outperformance'] = avg_outperformance
                
                # Debe superar al menos min_years para este benchmark
                if outperform_years < min_years:
                    beats_all_benchmarks = False
            
            if beats_all_benchmarks:
                sustained_outperformers.append(performance_summary)
        
        return sustained_outperformers

class StockAnalyzer:
    """Analizador principal de acciones"""
    
    def __init__(self, excel_file='Market_Cap_Ranked.xlsx'):
        self.excel_file = excel_file
        self.benchmarks = ['SPY', 'QQQ', 'SPYG']
        self.stock_symbols = self._load_stock_symbols()
        
    def _load_stock_symbols(self):
        """Carga los símbolos desde el archivo Excel"""
        try:
            print(f"📊 Cargando símbolos desde {self.excel_file}...")
            df = pd.read_excel(self.excel_file)
            
            print(f"Columnas disponibles: {list(df.columns)}")
            
            # Verificar que las columnas necesarias existan
            if 'Ticker' not in df.columns:
                raise ValueError("La columna 'Ticker' no se encontró en el Excel")
            
            if 'Market Cap Rank' not in df.columns:
                print("⚠️ Advertencia: 'Market Cap Rank' no encontrado, usando orden del archivo")
                df['Market Cap Rank'] = range(1, len(df) + 1)
            
            # Tomar los primeros 5769 stocks ordenados por Market Cap Rank
            df_sorted = df.sort_values('Market Cap Rank').head(5769)
            symbols = df_sorted['Ticker'].dropna().unique().tolist()
            
            # Limpiar símbolos (remover espacios, convertir a string)
            symbols = [str(symbol).strip().upper() for symbol in symbols if pd.notna(symbol) and str(symbol).strip()]
            
            print(f"✅ {len(symbols)} símbolos cargados exitosamente")
            print(f"Primeros 10: {symbols[:10]}")
            
            return symbols
            
        except Exception as e:
            print(f"❌ Error cargando archivo Excel: {e}")
            print("Usando símbolos de respaldo...")
            # Símbolos de respaldo del S&P 500
            return [
                'AAPL', 'MSFT', 'NVDA', 'AMZN', 'META', 'GOOGL', 'AVGO', 'TSLA', 'GOOG',
                'LLY', 'JPM', 'V', 'NFLX', 'XOM', 'MA', 'COST', 'WMT', 'PG', 'UNH',
                'JNJ', 'HD', 'ABBV', 'KO', 'PM', 'BAC', 'CRM', 'WFC', 'CSCO',
                'MCD', 'ORCL', 'CVX', 'ABT', 'IBM', 'GE', 'LIN', 'MRK', 'T'
            ]
    
    def run_analysis_period(self, years=5, min_outperform_percentage=0.8, ytd=False):
        """Ejecuta el análisis para un período específico"""
        
        current_date = datetime.now()
        current_year = current_date.year
        
        if ytd:
            # Análisis Year-to-Date
            start_date = f"{current_year}-01-01"
            end_date = current_date.strftime("%Y-%m-%d")
            period_name = f"YTD {current_year}"
            print(f"\n🚀 Iniciando análisis YTD {current_year} (01-enero hasta {current_date.strftime('%d-%B')})...")
        else:
            # Análisis histórico completo
            start_year = current_year - years
            end_year = current_year - 1
            start_date = f"{start_year}-01-01"
            end_date = f"{end_year}-12-31"
            period_name = f"{years} años"
            print(f"\n🚀 Iniciando análisis para {years} años ({start_year}-{end_year})...")
        
        # Crear fetcher para este período
        fetcher = DataFetcher(start_date, end_date)
        calculator = PerformanceCalculator()
        
        # 1. Obtener datos de benchmarks
        print(f"\n📊 Obteniendo datos de benchmarks para {years} años...")
        benchmark_data = fetcher.fetch_multiple_stocks(self.benchmarks)
        benchmark_returns = {}
        
        for benchmark, data in benchmark_data.items():
            if ytd:
                returns = {current_year: (data.iloc[-1] / data.iloc[0] - 1) * 100}
            else: 
                start_year = current_year - years
                end_year = current_year - 1
                returns = calculator.calculate_annual_returns(data, start_year, end_year)    
            benchmark_returns[benchmark] = returns
            if returns:
                avg_return = np.mean(list(returns.values()))
                print(f"{benchmark}: {avg_return:.2f}% promedio anual")
        
        # 2. Obtener datos de acciones
        print(f"\n📈 Obteniendo datos de {len(self.stock_symbols)} acciones...")
        stock_data = fetcher.fetch_multiple_stocks(self.stock_symbols)
        
        # 3. Calcular rendimientos anuales para todas las acciones
        print(f"\n🧮 Calculando rendimientos para {period_name}...")
        stock_returns = {}
        
        if ytd:
            # Para YTD, calcular rendimiento desde inicio de año
            for symbol, data in stock_data.items():
                if data is not None and not data.empty:
                    try:
                        start_price = data.iloc[0]
                        end_price = data.iloc[-1]
                        if start_price > 0:
                            ytd_return = (end_price / start_price - 1) * 100
                            stock_returns[symbol] = {current_year: ytd_return}
                    except Exception as e:
                        print(f"Error calculando YTD para {symbol}: {e}")
                        continue
        else:
            # Análisis histórico normal
            start_year = current_year - years
            end_year = current_year - 1
            for symbol, data in stock_data.items():
                returns = calculator.calculate_annual_returns(data, start_year, end_year)
                if returns and len(returns) >= max(1, int(years * 0.6)):  # Al menos 60% de los años
                    stock_returns[symbol] = returns
        
        print(f"Rendimientos calculados para {len(stock_returns)} acciones")
        
        # 4. Comparar contra benchmarks
        print(f"\n⚖️ Comparando contra benchmarks para {period_name}...")
        
        # Debug: Mostrar qué benchmarks tenemos
        print(f"Benchmarks disponibles: {list(benchmark_returns.keys())}")
        for bench, data in benchmark_returns.items():
            if data:
                print(f"{bench} tiene datos: {data}")
        
        comparator = BenchmarkComparator(benchmark_returns)
        
        if ytd:
            # Para YTD, ajustar criterio: solo necesita superar en el período actual
            filter_obj = OutperformanceFilter(comparator, min_years_percentage=1.0)  # 100% del tiempo (solo YTD)
            total_periods = 1
        else:
            filter_obj = OutperformanceFilter(comparator, min_outperform_percentage)
            total_periods = years
        
        outperformers = filter_obj.filter_sustained_outperformers(
            stock_returns, self.benchmarks, total_periods
        )
        
        # 5. Calcular métricas adicionales para TODAS las acciones
        print(f"\n📊 Calculando métricas adicionales para {period_name}...")
        results = []
        
        for symbol in stock_returns.keys():
            if symbol in stock_data:
                price_data = stock_data[symbol]
                returns = stock_returns[symbol]
                
                if ytd:
                    # Métricas específicas para YTD
                    ytd_return = list(returns.values())[0] if returns else 0
                    total_return = ytd_return
                    cagr = ytd_return  # Para YTD, es lo mismo
                    
                    # Métricas de riesgo (usar datos YTD - pero necesitan más datos)
                    if price_data is not None and len(price_data) > 20:  # Mínimo 20 días
                        # Para YTD, calcular Sharpe con datos diarios
                        daily_returns = price_data.pct_change().dropna()
                        if len(daily_returns) > 0:
                            excess_returns = daily_returns - (0.02/252)  # Risk-free diario
                            if daily_returns.std() > 0:
                                sharpe = (excess_returns.mean() / daily_returns.std()) * np.sqrt(252)
                            else:
                                sharpe = 0
                        else:
                            sharpe = 0
                        max_dd = calculator.calculate_max_drawdown(price_data)
                        volatility = calculator.calculate_volatility(price_data)
                    else:
                        sharpe = max_dd = volatility = 0
                    
                    period_suffix = "YTD"
                else:
                    # Métricas históricas normales
                    avg_return = np.mean(list(returns.values())) if returns else 0
                    total_return = calculator.calculate_total_return(price_data)
                    cagr = calculator.calculate_cagr(price_data, years)
                    sharpe = calculator.calculate_sharpe_ratio(returns)
                    max_dd = calculator.calculate_max_drawdown(price_data)
                    volatility = calculator.calculate_volatility(price_data)
                    
                    period_suffix = f"{years}Y"
                
                # Obtener años de outperformance para cada benchmark
                spy_years, spy_total, spy_outperf = comparator.count_outperformance_years(returns, 'SPY')
                qqq_years, qqq_total, qqq_outperf = comparator.count_outperformance_years(returns, 'QQQ')
                spyg_years, spyg_total, spyg_outperf = comparator.count_outperformance_years(returns, 'SPYG')
                
                result = {
                    'Symbol': symbol,
                    f'Return_{period_suffix}_%': round(ytd_return if ytd else avg_return, 2),
                    f'Total_Return_{period_suffix}_%': round(total_return, 2),
                    f'CAGR_{period_suffix}_%': round(cagr, 2),
                    'Beats_SPY': spy_years,
                    'Beats_QQQ': qqq_years,
                    'Beats_SPYG': spyg_years,
                    'SPY_Outperformance_%': round(spy_outperf, 2),
                    'QQQ_Outperformance_%': round(qqq_outperf, 2),
                    'SPYG_Outperformance_%': round(spyg_outperf, 2),
                    'Sharpe_Ratio': round(sharpe, 2),
                    'Max_Drawdown_%': round(max_dd, 2),
                    'Volatility_%': round(volatility, 2),
                    'Data_Periods': len(returns)
                }
                results.append(result)
        
        return results, benchmark_returns, outperformers

class ExcelExporter:
    """Módulo para exportar resultados a Excel con formato"""
    
    @staticmethod
    def create_formatted_excel(results_5y, results_10y, results_ytd, benchmark_5y, benchmark_10y, benchmark_ytd,
                             outperformers_5y, outperformers_10y, outperformers_ytd, filename="Stock_Analysis_Results.xlsx"):
        """Crea un archivo Excel formateado con múltiples hojas incluyendo YTD"""
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            
            # Hoja 1: Análisis YTD - PRIORIDAD #1
            if results_ytd:
                df_ytd = pd.DataFrame(results_ytd)
                df_ytd = df_ytd.sort_values('Return_YTD_%', ascending=False)
                df_ytd.to_excel(writer, sheet_name='Analysis_YTD_2025', index=False)
                
                worksheet_ytd = writer.sheets['Analysis_YTD_2025']
                ExcelExporter._format_worksheet(worksheet_ytd, "🔥 Análisis Year-to-Date 2025 (18-Jun)")
            
            # Hoja 2: Análisis 5 años
            if results_5y:
                df_5y = pd.DataFrame(results_5y)
                df_5y = df_5y.sort_values('CAGR_5Y_%', ascending=False)
                df_5y.to_excel(writer, sheet_name='Analysis_5_Years', index=False)
                
                worksheet_5y = writer.sheets['Analysis_5_Years']
                ExcelExporter._format_worksheet(worksheet_5y, "Análisis de Rendimiento - 5 Años")
            
            # Hoja 3: Análisis 10 años  
            if results_10y:
                df_10y = pd.DataFrame(results_10y)
                df_10y = df_10y.sort_values('CAGR_10Y_%', ascending=False)
                df_10y.to_excel(writer, sheet_name='Analysis_10_Years', index=False)
                
                worksheet_10y = writer.sheets['Analysis_10_Years']
                ExcelExporter._format_worksheet(worksheet_10y, "Análisis de Rendimiento - 10 Años")
            
            # Hoja 4: Outperformers YTD
            if outperformers_ytd and results_ytd:
                outperf_ytd_symbols = [item['symbol'] for item in outperformers_ytd]
                outperf_ytd_data = [r for r in results_ytd if r['Symbol'] in outperf_ytd_symbols]
                
                if outperf_ytd_data:
                    df_outperf_ytd = pd.DataFrame(outperf_ytd_data)
                    df_outperf_ytd = df_outperf_ytd.sort_values('Return_YTD_%', ascending=False)
                    df_outperf_ytd.to_excel(writer, sheet_name='Outperformers_YTD', index=False)
                    
                    worksheet_out_ytd = writer.sheets['Outperformers_YTD']
                    ExcelExporter._format_worksheet(worksheet_out_ytd, "🚀 Outperformers YTD 2025", highlight=True)
            
            # Hoja 5: Outperformers 5 años
            if outperformers_5y:
                outperf_5y_symbols = [item['symbol'] for item in outperformers_5y]
                outperf_5y_data = [r for r in results_5y if r['Symbol'] in outperf_5y_symbols]
                
                if outperf_5y_data:
                    df_outperf_5y = pd.DataFrame(outperf_5y_data)
                    df_outperf_5y = df_outperf_5y.sort_values('CAGR_5Y_%', ascending=False)
                    df_outperf_5y.to_excel(writer, sheet_name='Outperformers_5Y', index=False)
                    
                    worksheet_out5 = writer.sheets['Outperformers_5Y']
                    ExcelExporter._format_worksheet(worksheet_out5, "Outperformers Sostenidos - 5 Años", highlight=True)
            
            # Hoja 6: Outperformers 10 años
            if outperformers_10y:
                outperf_10y_symbols = [item['symbol'] for item in outperformers_10y]
                outperf_10y_data = [r for r in results_10y if r['Symbol'] in outperf_10y_symbols]
                
                if outperf_10y_data:
                    df_outperf_10y = pd.DataFrame(outperf_10y_data)
                    df_outperf_10y = df_outperf_10y.sort_values('CAGR_10Y_%', ascending=False)
                    df_outperf_10y.to_excel(writer, sheet_name='Outperformers_10Y', index=False)
                    
                    worksheet_out10 = writer.sheets['Outperformers_10Y']
                    ExcelExporter._format_worksheet(worksheet_out10, "Outperformers Sostenidos - 10 Años", highlight=True)
            
            # Hoja 7: Resumen de Benchmarks y Top Performers
            summary_data = []
            
            # Benchmarks YTD
            if benchmark_ytd:
                for benchmark, returns in benchmark_ytd.items():
                    if returns:
                        ytd_return = list(returns.values())[0]
                        summary_data.append({
                            'Category': 'Benchmark',
                            'Symbol': benchmark,
                            'Period': 'YTD 2025',
                            'Return_%': round(ytd_return, 2),
                            'Type': 'ETF'
                        })
            
            # Top 3 YTD performers
            if results_ytd:
                top_ytd = sorted(results_ytd, key=lambda x: x['Return_YTD_%'], reverse=True)[:3]
                for i, stock in enumerate(top_ytd, 1):
                    summary_data.append({
                        'Category': f'Top_{i}_YTD',
                        'Symbol': stock['Symbol'],
                        'Period': 'YTD 2025',
                        'Return_%': stock['Return_YTD_%'],
                        'Type': 'Stock'
                    })
            
            # Benchmarks 5 años
            if benchmark_5y:
                for benchmark, returns in benchmark_5y.items():
                    if returns:
                        avg_return = np.mean(list(returns.values()))
                        summary_data.append({
                            'Category': 'Benchmark',
                            'Symbol': benchmark,
                            'Period': '5 Years',
                            'Return_%': round(avg_return, 2),
                            'Type': 'ETF'
                        })
            
            # Top 3 performers 5 años
            if results_5y:
                top_5y = sorted(results_5y, key=lambda x: x['CAGR_5Y_%'], reverse=True)[:3]
                for i, stock in enumerate(top_5y, 1):
                    summary_data.append({
                        'Category': f'Top_{i}_5Y',
                        'Symbol': stock['Symbol'],
                        'Period': '5 Years',
                        'Return_%': stock['CAGR_5Y_%'],
                        'Type': 'Stock'
                    })
            
            # Benchmarks 10 años  
            if benchmark_10y:
                for benchmark, returns in benchmark_10y.items():
                    if returns:
                        avg_return = np.mean(list(returns.values()))
                        summary_data.append({
                            'Category': 'Benchmark',
                            'Symbol': benchmark,
                            'Period': '10 Years', 
                            'Return_%': round(avg_return, 2),
                            'Type': 'ETF'
                        })
            
            # Top 3 performers 10 años
            if results_10y:
                top_10y = sorted(results_10y, key=lambda x: x['CAGR_10Y_%'], reverse=True)[:3]
                for i, stock in enumerate(top_10y, 1):
                    summary_data.append({
                        'Category': f'Top_{i}_10Y',
                        'Symbol': stock['Symbol'],
                        'Period': '10 Years',
                        'Return_%': stock['CAGR_10Y_%'],
                        'Type': 'Stock'
                    })
            
            if summary_data:
                df_summary = pd.DataFrame(summary_data)
                df_summary.to_excel(writer, sheet_name='Summary_All_Periods', index=False)
                
                worksheet_summary = writer.sheets['Summary_All_Periods']
                ExcelExporter._format_worksheet(worksheet_summary, "📊 Resumen: Benchmarks + Top Performers")
        
        print(f"✅ Archivo Excel creado: {filename}")
        return filename
    
    @staticmethod
    def _format_worksheet(worksheet, title, highlight=False):
        """Aplica formato a una hoja de Excel"""
        
        # Título
        worksheet.insert_rows(1)
        worksheet['A1'] = title
        worksheet['A1'].font = Font(bold=True, size=14)
        worksheet['A1'].fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
        worksheet['A1'].font = Font(bold=True, size=14, color="FFFFFF")
        
        # Encabezados (ahora en fila 2)
        header_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
        header_font = Font(bold=True)
        
        for cell in worksheet[2]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center")
        
        # Si es hoja de outperformers, destacar más
        if highlight:
            highlight_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
            for row in worksheet.iter_rows(min_row=3, max_row=worksheet.max_row):
                for cell in row:
                    cell.fill = highlight_fill
        
        # Ajustar ancho de columnas
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            
            adjusted_width = min(max_length + 2, 20)
            worksheet.column_dimensions[column_letter].width = adjusted_width

def main():
    """Función principal que ejecuta todo el análisis incluyendo YTD"""
    try:
        print("🚀 INICIANDO ANÁLISIS COMPLETO DE ACCIONES")
        print("=" * 60)
        
        # Crear analizador
        analyzer = StockAnalyzer('Market_Cap_Ranked.xlsx')
        
        # Análisis YTD 2025 - PRIORIDAD #1
        print("\n" + "="*25 + " ANÁLISIS YTD 2025 " + "="*25)
        results_ytd, benchmark_ytd, outperformers_ytd = analyzer.run_analysis_period(ytd=True)
        
        # Análisis para 5 años
        print("\n" + "="*30 + " ANÁLISIS 5 AÑOS " + "="*30)
        results_5y, benchmark_5y, outperformers_5y = analyzer.run_analysis_period(years=5)
        
        # Análisis para 10 años
        print("\n" + "="*30 + " ANÁLISIS 10 AÑOS " + "="*30)
        results_10y, benchmark_10y, outperformers_10y = analyzer.run_analysis_period(years=10)
        
        # Crear archivo Excel con YTD incluido
        print("\n📊 Creando archivo Excel con resultados incluyendo YTD...")
        filename = ExcelExporter.create_formatted_excel(
            results_5y, results_10y, results_ytd, 
            benchmark_5y, benchmark_10y, benchmark_ytd,
            outperformers_5y, outperformers_10y, outperformers_ytd
        )
        
        # Mostrar resumen DCA ESTRATÉGICO
        print(f"\n{'='*60}")
        print("💰 RECOMENDACIONES PARA ESTRATEGIA DCA")
        print(f"{'='*60}")
        
        # Analizar consistencia a través de períodos
        dca_candidates = []
        
        if results_ytd and results_5y and results_10y:
            # Crear diccionario de acciones con datos completos
            stocks_analysis = {}
            
            # Recopilar datos YTD
            for stock in results_ytd:
                symbol = stock['Symbol']
                stocks_analysis[symbol] = {
                    'ytd_return': stock['Return_YTD_%'],
                    'ytd_beats_spy': stock['Beats_SPY'],
                    'ytd_sharpe': stock['Sharpe_Ratio'],
                    'ytd_volatility': stock['Volatility_%']
                }
            
            # Agregar datos 5 años
            for stock in results_5y:
                symbol = stock['Symbol']
                if symbol in stocks_analysis:
                    stocks_analysis[symbol].update({
                        'cagr_5y': stock['CAGR_5Y_%'],
                        'sharpe_5y': stock['Sharpe_Ratio'],
                        'beats_spy_5y': stock['Beats_SPY'],
                        'volatility_5y': stock['Volatility_%'],
                        'max_dd_5y': stock['Max_Drawdown_%']
                    })
            
            # Agregar datos 10 años
            for stock in results_10y:
                symbol = stock['Symbol']
                if symbol in stocks_analysis:
                    stocks_analysis[symbol].update({
                        'cagr_10y': stock['CAGR_10Y_%'],
                        'sharpe_10y': stock['Sharpe_Ratio'],
                        'beats_spy_10y': stock['Beats_SPY'],
                        'volatility_10y': stock['Volatility_%'],
                        'max_dd_10y': stock['Max_Drawdown_%']
                    })
            
            # Filtrar candidatos DCA (deben tener datos completos)
            for symbol, data in stocks_analysis.items():
                if all(key in data for key in ['cagr_5y', 'cagr_10y', 'ytd_return']):
                    # Criterios DCA
                    consistent_performer = (
                        data.get('cagr_10y', 0) > 15 and  # CAGR 10Y > 15%
                        data.get('cagr_5y', 0) > 20 and   # CAGR 5Y > 20%
                        data.get('sharpe_10y', 0) > 0.8   # Sharpe decente
                    )
                    
                    if consistent_performer:
                        # Score DCA (combina rendimiento y estabilidad)
                        dca_score = (
                            data.get('cagr_10y', 0) * 0.4 +  # 40% peso a consistencia 10Y
                            data.get('cagr_5y', 0) * 0.3 +   # 30% peso a performance 5Y
                            data.get('sharpe_10y', 0) * 20 + # 20% peso a Sharpe (x20 para escalar)
                            (100 - data.get('volatility_10y', 100)) * 0.1  # 10% peso a baja volatilidad
                        )
                        
                        dca_candidates.append({
                            'symbol': symbol,
                            'dca_score': dca_score,
                            'cagr_10y': data.get('cagr_10y', 0),
                            'cagr_5y': data.get('cagr_5y', 0),
                            'ytd_return': data.get('ytd_return', 0),
                            'sharpe_10y': data.get('sharpe_10y', 0),
                            'volatility_10y': data.get('volatility_10y', 0),
                            'max_dd_10y': data.get('max_dd_10y', 0)
                        })
            
            # Ordenar por DCA score
            dca_candidates.sort(key=lambda x: x['dca_score'], reverse=True)
            
            # Mostrar recomendaciones DCA
            if dca_candidates:
                print(f"\n🎯 TOP 5 CANDIDATOS PARA DCA (ordenados por consistencia + rendimiento):")
                print("=" * 90)
                for i, candidate in enumerate(dca_candidates[:5], 1):
                    symbol = candidate['symbol']
                    print(f"{i}. {symbol:6} | CAGR 10Y: {candidate['cagr_10y']:6.1f}% | "
                          f"CAGR 5Y: {candidate['cagr_5y']:6.1f}% | YTD: {candidate['ytd_return']:6.1f}% | "
                          f"Sharpe: {candidate['sharpe_10y']:4.2f} | Vol: {candidate['volatility_10y']:5.1f}%")
                
                # Recomendación de portafolio DCA
                print(f"\n💡 PORTAFOLIO DCA RECOMENDADO:")
                print("=" * 50)
                
                if len(dca_candidates) >= 3:
                    top_3 = dca_candidates[:3]
                    print(f"🥇 CORE HOLDING (40%): {top_3[0]['symbol']} - Mejor balance riesgo/retorno")
                    print(f"🥈 GROWTH COMPONENT (35%): {top_3[1]['symbol']} - Sólido crecimiento")
                    print(f"🥉 DIVERSIFICATION (25%): {top_3[2]['symbol']} - Diversificación")
                    
                    # Ejemplo práctico DCA
                    print(f"\n📅 IMPLEMENTACIÓN DCA MENSUAL (ejemplo $1,000/mes):")
                    print(f"   • ${400}/mes → {top_3[0]['symbol']} (día 1 de cada mes)")
                    print(f"   • ${350}/mes → {top_3[1]['symbol']} (día 1 de cada mes)")
                    print(f"   • ${250}/mes → {top_3[2]['symbol']} (día 1 de cada mes)")
                    
                    # Proyección conservadora
                    conservative_cagr = min(top_3[0]['cagr_10y'], top_3[1]['cagr_10y'], top_3[2]['cagr_10y'])
                    print(f"\n📊 PROYECCIÓN CONSERVADORA (usando CAGR más bajo: {conservative_cagr:.1f}%):")
                    years_projection = 10
                    monthly_investment = 1000
                    total_invested = monthly_investment * 12 * years_projection
                    # Fórmula para valor futuro de anualidades con crecimiento
                    monthly_rate = conservative_cagr / 100 / 12
                    if monthly_rate > 0:
                        future_value = monthly_investment * (((1 + monthly_rate) ** (12 * years_projection) - 1) / monthly_rate)
                        total_gain = future_value - total_invested
                        print(f"   • Inversión total {years_projection} años: ${total_invested:,.0f}")
                        print(f"   • Valor proyectado: ${future_value:,.0f}")
                        print(f"   • Ganancia potencial: ${total_gain:,.0f}")
                
                # Alertas y consideraciones
                print(f"\n⚠️ CONSIDERACIONES DCA:")
                print("   • Rebalancear anualmente si alguna posición supera 45%")
                print("   • Revisar análisis cada 3 meses (usar este mismo script)")
                print("   • En crashes >30%: considera ACELERAR el DCA si tienes liquidez")
                print("   • Mantener disciplina: NO pausar DCA en mercados bajistas")
                
                # Red flags actuales
                ytd_underperformers = [c for c in dca_candidates[:3] if c['ytd_return'] < 0]
                if ytd_underperformers:
                    print(f"\n🚨 ALERTAS YTD 2025:")
                    for stock in ytd_underperformers:
                        print(f"   • {stock['symbol']}: {stock['ytd_return']:.1f}% YTD - Investigar si hay cambios fundamentales")
        
        # Benchmarks para referencia
        if benchmark_ytd:
            print(f"\n📊 BENCHMARKS YTD 2025 (para comparación):")
            for benchmark, returns in benchmark_ytd.items():
                if returns:
                    ytd_return = list(returns.values())[0]
                    print(f"   • {benchmark}: {ytd_return:.1f}%")
        
        print(f"\n✅ Análisis completado. Resultados guardados en: {filename}")
        print(f"\n🔥 PRÓXIMA REVISIÓN: Septiembre 2025 (trimestral)")
        print(f"📋 COMANDO: python {os.path.basename(__file__)}")
        
        return filename, results_ytd, results_5y, results_10y, outperformers_ytd, outperformers_5y, outperformers_10y
    
        
    except Exception as e:
        print(f"❌ Error en el análisis: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None, None, None

if __name__ == "__main__":
    filename, results_ytd, results_5y, results_10y, outperformers_ytd, outperformers_5y, outperformers_10y = main() 