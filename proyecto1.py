import boto3
import os
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import unquote_plus
from datetime import datetime
import re
import json
import csv



s3 = boto3.client('s3')

def app(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'])

    if not key.endswith('.html'):
        print(f"Ignorado: archivo no es HTML ({key})")
        return {
            'statusCode': 200,
            'body': f'Se ignoró el archivo: {key}'
        }

    local_file = '/tmp/page.html'
    s3.download_file(bucket, key, local_file)

    with open(local_file, 'r', encoding='utf-8') as f:
        html = f.read()

    soup = BeautifulSoup(html, 'html.parser')

    # Determinar el periódico por el nombre del archivo
    if 'eltiempo' in key:
        periodico = 'eltiempo'
        data = parse_el_tiempo(html)
    elif 'publimetro' in key :
        periodico = 'publimetro'
        data = extraer_noticias_publimetro(html)
    else:
        raise ValueError('No se pudo determinar el periódico del archivo.')

    if not data:
        raise ValueError("No se extrajo ninguna noticia.")

    df = pd.DataFrame(data)

    filename = key.split('/')[-1].replace('.html', '')
    match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
    if not match:
        raise ValueError(f"No se encontró una fecha válida en el nombre del archivo: {filename}")
    
    fecha_str = match.group(1)
    fecha = datetime.strptime(fecha_str, '%Y-%m-%d')

    output_key = f"final/periodico={periodico}/year={fecha.year}/month={fecha.month:02d}/day={fecha.day:02d}/titulares.csv"
    output_file = '/tmp/titulares.csv'
    df.to_csv(output_file, index=False)
    s3.upload_file(output_file, bucket, output_key)
    
    time.sleep(20)
    
    client = boto3.client('lambda')
    response = client.invoke(
        FunctionName='lambda-333-dev3',
        InvocationType='Event',  # Usa 'RequestResponse' si necesitas esperar la respuesta
        Payload=b'{}'  # Puedes pasar datos aquí si lo necesitas
    )

    print("Invocación enviada a la tercera Lambda.")

    return {
        'statusCode': 200,
        'body': f'Archivo procesado y guardado en {output_key}'
    }

# -----------------------------
# FUNCIONES EXTRACTORAS NUEVAS
# -----------------------------

def parse_el_tiempo(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    noticias = []

    enlaces_noticias = soup.find_all('a', href=True)
    for enlace in enlaces_noticias:
        href = enlace.get('href', '')
        if (href.startswith('/') and len(href.split('/')) >= 3 and
            not any(x in href for x in ['/images/', '/assets/', '/css/', '/js/', '.jpg', '.png', '.gif'])):
            titular = enlace.get_text().strip()
            if titular and len(titular) > 10:
                partes_url = href.strip('/').split('/')
                categoria = partes_url[0] if partes_url else 'General'
                titular_limpio = re.sub(r'\s+', ' ', titular).strip()
                categoria_limpia = categoria.replace('-', ' ').title()
                url_completa = f"https://www.eltiempo.com{href}"

                noticia = {
                    'categoria': categoria_limpia,
                    'titulo': titular_limpio,
                    'enlace': url_completa
                }

                if noticia not in noticias:
                    noticias.append(noticia)

    selectores_noticias = [
        'article a[href]',
        '.noticia a[href]',
        '.articulo a[href]',
        '[class*="headline"] a[href]',
        '[class*="title"] a[href]',
        '[class*="news"] a[href]'
    ]

    for selector in selectores_noticias:
        elementos = soup.select(selector)
        for elemento in elementos:
            href = elemento.get('href', '')
            titular = elemento.get_text().strip()
            if (href and titular and len(titular) > 10 and
                href.startswith('/') and len(href.split('/')) >= 3):
                partes_url = href.strip('/').split('/')
                categoria = partes_url[0] if partes_url else 'General'
                titular_limpio = re.sub(r'\s+', ' ', titular).strip()
                categoria_limpia = categoria.replace('-', ' ').title()
                url_completa = f"https://www.eltiempo.com{href}"

                noticia = {
                    'categoria': categoria_limpia,
                    'titulo': titular_limpio,
                    'enlace': url_completa
                }

                if noticia not in noticias:
                    noticias.append(noticia)

    scripts_jsonld = soup.find_all('script', type='application/ld+json')
    for script in scripts_jsonld:
        try:
            data = json.loads(script.string)
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get('@type') in ['NewsArticle', 'ReportageNewsArticle']:
                        titular = item.get('headline', '')
                        url = item.get('mainEntityOfPage', {}).get('@id', '') if isinstance(item.get('mainEntityOfPage'), dict) else ''
                        if titular and url:
                            partes_url = url.replace('https://www.eltiempo.com/', '').split('/')
                            categoria = partes_url[0] if partes_url else 'General'
                            categoria_limpia = categoria.replace('-', ' ').title()
                            noticia = {
                                'categoria': categoria_limpia,
                                'titulo': titular,
                                'enlace': url
                            }
                            if noticia not in noticias:
                                noticias.append(noticia)
        except:
            continue

    # Filtrar duplicados por titular
    noticias_filtradas = []
    titulares_vistos = set()
    for noticia in noticias:
        normalizado = re.sub(r'[^\w\s]', '', noticia['titulo'].lower())
        if normalizado not in titulares_vistos and len(noticia['titulo']) > 15:
            titulares_vistos.add(normalizado)
            noticias_filtradas.append(noticia)

    return noticias_filtradas

# Mantén esta función como está si quieres seguir extrayendo de El Espectador

def extraer_noticias_publimetro(html_content):
    """
    Extrae información de noticias del HTML de Publimetro y retorna una lista de noticias.

    Args:
        html_content (str): Contenido HTML de la página.
    Returns:
        list[dict]: Lista de noticias con categoría, titular y link completo.
    """
    BASE_URL = "https://www.publimetro.co"
    soup = BeautifulSoup(html_content, 'html.parser')
    noticias = []

    def limpiar_texto(texto):
        return re.sub(r'\s+', ' ', texto.strip()) if texto else ""

    def completar_link(href):
        if href.startswith("http"):
            return href
        return BASE_URL + href

    def extraer_categoria(elemento):
        categoria_elem = elemento.find('span', class_='c-overline')
        if categoria_elem:
            return limpiar_texto(categoria_elem.get_text())

        parent = elemento.find_parent()
        while parent:
            categoria_elem = parent.find('span', class_='c-overline')
            if categoria_elem:
                return limpiar_texto(categoria_elem.get_text())
            parent = parent.find_parent()

        links = elemento.find_all('a', href=True)
        for link in links:
            href = link['href']
            if '/deportes/' in href:
                return 'Deportes'
            if '/entretenimiento/' in href:
                return 'Entretenimiento'
            if '/noticias/' in href:
                return 'Noticias'
            if '/barranquilla/' in href:
                return 'Barranquilla'

        return 'Sin categoría'

    # Noticias principales
    for noticia in soup.find_all('article', class_='b-top-table-list-xl'):
        titulo_elem = noticia.find('h2', class_='c-heading')
        link_elem = titulo_elem.find('a', class_='c-link') if titulo_elem else None
        if link_elem:
            noticias.append({
                'categoria': extraer_categoria(noticia),
                'titular': limpiar_texto(link_elem.get_text()),
                'link': completar_link(link_elem.get('href'))
            })

    # Sección "Para entretenerse"
    seccion_entretenimiento = soup.find('div', class_='b-card-list')
    if seccion_entretenimiento:
        noticia_main = seccion_entretenimiento.find('article', class_='b-card-list__main-item')
        if noticia_main:
            titulo_elem = noticia_main.find('h3', class_='c-heading')
            link_elem = titulo_elem.find('a', class_='c-link') if titulo_elem else None
            if link_elem:
                noticias.append({
                    'categoria': extraer_categoria(noticia_main),
                    'titular': limpiar_texto(link_elem.get_text()),
                    'link': completar_link(link_elem.get('href'))
                })

        for noticia in seccion_entretenimiento.find_all('article', class_='b-card-list__secondary-item'):
            titulo_elem = noticia.find('h3', class_='c-heading')
            link_elem = titulo_elem.find('a', class_='c-link') if titulo_elem else None
            if link_elem:
                noticias.append({
                    'categoria': extraer_categoria(noticia),
                    'titular': limpiar_texto(link_elem.get_text()),
                    'link': completar_link(link_elem.get('href'))
                })

    # Lista pequeña
    for noticia in soup.find_all('article', class_='b-top-table-list-small'):
        titulo_elem = noticia.find('h2', class_='c-heading')
        link_elem = titulo_elem.find('a', class_='c-link') if titulo_elem else None
        if link_elem:
            noticias.append({
                'categoria': extraer_categoria(noticia),
                'titular': limpiar_texto(link_elem.get_text()),
                'link': completar_link(link_elem.get('href'))
            })

    # Resultados
    for seccion in soup.find_all('div', class_='b-results-list'):
        for enlace in seccion.find_all('a', class_='c-link', href=True):
            if enlace.get('aria-hidden') == 'true' or enlace.get('tabindex') == '-1':
                continue
            titulo = limpiar_texto(enlace.get_text())
            if titulo:
                noticias.append({
                    'categoria': extraer_categoria(enlace.find_parent()),
                    'titular': titulo,
                    'link': completar_link(enlace['href'])
                })

    # Búsqueda general
    for enlace in soup.find_all('a', class_='c-link', href=True):
        if (enlace.get('aria-hidden') == 'true' or
            enlace.get('tabindex') == '-1' or
            not enlace['href'].startswith('/')):
            continue

        if enlace.find_parent(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            titulo = limpiar_texto(enlace.get_text())
            if titulo and titulo not in [n['titular'] for n in noticias]:
                noticias.append({
                    'categoria': extraer_categoria(enlace.find_parent()),
                    'titular': titulo,
                    'link': completar_link(enlace['href'])
                })

    # Eliminar duplicados
    noticias_unicas = []
    titulos_vistos = set()
    for noticia in noticias:
        if noticia['titular'] not in titulos_vistos:
            noticias_unicas.append(noticia)
            titulos_vistos.add(noticia['titular'])

    return noticias_unicas

