# -*- coding: utf-8 -*-
{
    'name': 'POS Receipt 58mm Thermal Printer',
    'version': '17.0.1.0.0',
    'category': 'Point of Sale',
    'summary': 'Tickets POS para impresoras térmicas de 58mm - Ajuste completo de contenido',
    'description': '''
        Personaliza las plantillas de tickets del POS para impresoras
        térmicas de 58mm, asegurando que todo el contenido aparezca completo.
        
        Características:
        - Ajusta el ancho del ticket a 48mm (ancho efectivo de impresoras 58mm)
        - Optimiza fuentes y tamaños para impresión térmica
        - Asegura que el texto se ajuste correctamente sin cortes
        - Optimiza tablas y columnas para el ancho disponible
    ''',
    'author': 'Rendon Industries',
    'website': 'https://rendonindustries.com',
    'depends': ['point_of_sale'],
    'data': [],
    'assets': {
        'point_of_sale.assets_prod': [
            'pos_receipt_58mm/static/src/css/receipt_58mm.css',
        ],
    },
    'installable': True,
    'application': False,
    'auto_install': False,
    'license': 'LGPL-3',
}
