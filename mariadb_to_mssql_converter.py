import re
import pyodbc
from typing import List, Dict, Optional
import logging
import os
import chardet

class MariaDBToMSSQLConverter:
    def __init__(self, server: str, database: str,
                 batch_size: int = 100, continue_on_error: bool = False,
                 odbc_driver: str = 'ODBC Driver 17 for SQL Server',
                 default_encoding: str = 'utf-8'):

        # Формируем строку подключения
        self.connection_string = (
            f'DRIVER={{{odbc_driver}}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            'Trusted_Connection=yes;'
            'Charset=UTF-8;'
        )
        
        self.batch_size = batch_size
        self.continue_on_error = continue_on_error
        self.default_encoding = default_encoding
        
        # Настройка логирования
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Хранилище структур таблиц
        self.table_structures: Dict[str, Dict] = {}
        
        # Маппинг оригинальных имен на уникальные
        self.original_to_unique: Dict[str, List[str]] = {}
        
        # Счетчик INSERT для каждого оригинального имени
        self.insert_counter: Dict[str, int] = {}
        
        # Типы данных MariaDB -> MSSQL
        self.type_mapping = {
            'int': 'INT',
            'tinyint': 'TINYINT',
            'smallint': 'SMALLINT',
            'mediumint': 'INT',
            'bigint': 'BIGINT',
            'float': 'FLOAT',
            'double': 'FLOAT',
            'decimal': 'DECIMAL(18,0)',
            'varchar': 'NVARCHAR',
            'char': 'NCHAR',
            'text': 'NVARCHAR(MAX)',
            'longtext': 'NVARCHAR(MAX)',
            'mediumtext': 'NVARCHAR(MAX)',
            'tinytext': 'NVARCHAR(255)',
            'blob': 'NVARCHAR(MAX)', # использовать VARBINARY(MAX) для бинарных данных
            'longblob': 'NVARCHAR(MAX)', # использовать VARBINARY(MAX) для бинарных данных
            'mediumblob': 'NVARCHAR(MAX)', # использовать VARBINARY(MAX) для бинарных данных
            'tinyblob': 'NVARCHAR(MAX)', # использовать VARBINARY(MAX) для бинарных данных
            'datetime': 'DATETIME2',
            'timestamp': 'DATETIME2',
            'date': 'DATE',
            'time': 'TIME',
            'year': 'INT',
            'bool': 'BIT',
            'boolean': 'BIT',
            'set': 'NVARCHAR(MAX)',
            'enum': 'NVARCHAR(10)'
        }

    def detect_encoding(self, file_path: str) -> str:
        """Определение кодировки с семплированием по всему файлу"""
        
        self.logger.info("=" * 60)
        self.logger.info("🔍 ОПРЕДЕЛЕНИЕ КОДИРОВКИ ФАЙЛА ")
        self.logger.info("=" * 60)
        
        # 1. Семплирование по всему файлу
        samples = []
        file_size = os.path.getsize(file_path)
        
        with open(file_path, 'rb') as f:
            # Начало файла (важно для BOM и заголовков)
            samples.append(f.read(8192))
            
            if file_size > 50000:
                # Середина файла
                f.seek(file_size // 2)
                samples.append(f.read(8192))
                
                # Конец файла (осторожно с маленькими файлами)
                if file_size > 8192:
                    f.seek(-8192, os.SEEK_END)
                    samples.append(f.read(8192))
                else:
                    f.seek(0)
                    samples.append(f.read(8192))
        
        # 2. Объединяем семплы
        raw_data = b''.join(samples)
        
        # 3. Используем стандартный chardet
        result = chardet.detect(raw_data)
        detected_encoding = result.get('encoding', '').lower()
        confidence = result.get('confidence', 0.0)
        
        self.logger.info(f"📊 Определена кодировка: {detected_encoding} ")
        self.logger.info(f"📊 Уверенность: {confidence:.2%} ")
        
        # 4. Дополнительная проверка на ложный ASCII
        if detected_encoding == 'ascii' and confidence > 0.9:
            self.logger.info("⚠️ Обнаружен ASCII, но проверяем весь файл на UTF-8...")
            
            with open(file_path, 'rb') as f:
                # Читаем весь файл маленькими кусками
                found_non_ascii = False
                while True:
                    chunk = f.read(4096)
                    if not chunk:
                        break
                    for byte in chunk:
                        if byte > 127:  # Нашли не-ASCII
                            found_non_ascii = True
                            break
                    if found_non_ascii:
                        break
                
                if found_non_ascii:
                    self.logger.info("✅ Найден не-ASCII байт, переключаемся на UTF-8 ")
                    return 'utf-8'
                else:
                    self.logger.info("❌ Не-ASCII байтов не найдено, оставляем ASCII ")

        # 4.1. Дополнительная проверка на ложный UTF-7
        if detected_encoding == 'utf-7':
            self.logger.info("⚠️ chardet определил UTF-7, но для SQL дампов это маловероятно ")
            self.logger.info("🔄 Принудительно используем UTF-8 ")
            return 'utf-8'
        
        # 5. Fallback на BOM
        with open(file_path, 'rb') as f:
            bom = f.read(4)
            if bom.startswith(b'\xef\xbb\xbf'):
                self.logger.info("📌 Обнаружен UTF-8 BOM ")
                return 'utf-8-sig'
            elif bom.startswith(b'\xff\xfe') or bom.startswith(b'\xfe\xff'):
                self.logger.info("📌 Обнаружен UTF-16 BOM ")
                return 'utf-16'
            elif bom.startswith(b'\xff\xfe\x00\x00') or bom.startswith(b'\x00\x00\xfe\xff'):
                self.logger.info("📌 Обнаружен UTF-32 BOM ")
                return 'utf-32'
        
        # 6. Обработка случая, когда encoding = None
        if detected_encoding is None:
            self.logger.warning("⚠️ Не удалось определить кодировку, пробуем UTF-8 ")
            try:
                with open(file_path, 'r', encoding='utf-8') as test_file:
                    test_file.read(100)
                self.logger.info("✅ UTF-8 подходит ")
                return 'utf-8'
            except UnicodeDecodeError:
                self.logger.warning(f"❌ UTF-8 не подходит, используем default = {self.default_encoding} ")
                return self.default_encoding
        
        # 7. Доверие только при высокой уверенности
        if confidence < 0.7:
            self.logger.warning(f"⚠️ Низкая уверенность ({confidence:.2%}), пробуем utf-8 ")
            # Пробуем декодировать как UTF-8
            try:
                with open(file_path, 'r', encoding='utf-8') as test_file:
                    test_file.read(100)
                self.logger.info("✅ UTF-8 подходит, используем его ")
                return 'utf-8'
            except UnicodeDecodeError:
                self.logger.warning(f"❌ UTF-8 не подходит, оставляем {detected_encoding} ")
                return detected_encoding
        
        return detected_encoding

    def connect_to_mssql(self):
        """Установка соединения с MSSQL"""

        try:
            conn = pyodbc.connect(self.connection_string, autocommit=False)
            self.logger.info("✅ Успешное подключение к MSSQL ")
            return conn
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения: {e} ")
            raise

    def escape_identifier(self, name: str) -> str:
        """Экранирует идентификатор SQL Server, если это зарезервированное слово"""
        
        # Список зарезервированных ключевых слов SQL Server
        reserved_keywords = {
            'add', 'all', 'alter', 'and', 'any', 'as', 'asc', 'authorization', 'backup', 'begin',
            'between', 'break', 'browse', 'bulk', 'by', 'cascade', 'case', 'check', 'checkpoint',
            'close', 'clustered', 'coalesce', 'collate', 'column', 'commit', 'compute', 'constraint',
            'contains', 'containstable', 'continue', 'convert', 'create', 'cross', 'current',
            'current_date', 'current_time', 'current_timestamp', 'current_user', 'cursor',
            'database', 'dbcc', 'deallocate', 'declare', 'default', 'delete', 'deny', 'desc',
            'disk', 'distinct', 'distributed', 'double', 'drop', 'dump', 'else', 'end', 'errlvl',
            'escape', 'except', 'exec', 'execute', 'exists', 'exit', 'external', 'fetch', 'file',
            'fillfactor', 'for', 'foreign', 'freetext', 'freetexttable', 'from', 'full', 'function',
            'goto', 'grant', 'group', 'having', 'holdlock', 'identity', 'identity_insert',
            'identitycol', 'if', 'in', 'index', 'inner', 'insert', 'intersect', 'into', 'is',
            'join', 'key', 'kill', 'left', 'like', 'lineno', 'load', 'merge', 'national', 'nocheck',
            'nonclustered', 'not', 'null', 'nullif', 'of', 'off', 'offsets', 'on', 'open',
            'opendatasource', 'openquery', 'openrowset', 'openxml', 'option', 'or', 'order',
            'outer', 'over', 'percent', 'pivot', 'plan', 'precision', 'primary', 'print', 'proc',
            'procedure', 'public', 'raiserror', 'read', 'readtext', 'reconfigure', 'references',
            'replication', 'restore', 'restrict', 'return', 'revert', 'revoke', 'right', 'rollback',
            'rowcount', 'rowguidcol', 'rule', 'save', 'schema', 'securityaudit', 'select',
            'semantickeyphrasetable', 'semanticsimilaritydetailstable', 'semanticsimilaritytable',
            'session_user', 'set', 'setuser', 'shutdown', 'some', 'statistics', 'system_user',
            'table', 'tablesample', 'textsize', 'then', 'to', 'top', 'tran', 'transaction',
            'trigger', 'truncate', 'try_convert', 'tsequal', 'union', 'unique', 'unpivot',
            'update', 'updatetext', 'use', 'user', 'values', 'varying', 'view', 'waitfor',
            'when', 'where', 'while', 'with', 'within group', 'writetext'
        }
        
        # Всегда экранируем зарезервированные слова
        if name.lower() in reserved_keywords:
            return f"[{name}]"
        
        # Также экранируем, если имя начинается с цифры или содержит пробелы
        if name and (name[0].isdigit() or ' ' in name or '-' in name):
            return f"[{name}]"
        
        # Иначе просто возвращаем имя
        return name

    def drop_table_if_exists(self, table_name: str, cursor) -> bool:
        """Удаляет таблицу, если она существует"""
        
        try:
            escaped_table_name = f"[{table_name}]"
            cursor.execute(f"""
                IF OBJECT_ID('{escaped_table_name}', 'U') IS NOT NULL DROP TABLE {escaped_table_name} """)
            cursor.commit()
            self.logger.info(f"  🗑️ Таблица {escaped_table_name} удалена (если была ранее создана) ")
            return True
        except Exception as e:
            self.logger.error(f"  ❌ Ошибка удаления: {e} ")
            return False

    def get_unique_table_name(self, original_name: str) -> str:
        """Генерирует уникальное имя таблицы при создании CREATE TABLE"""
        
        if original_name not in self.original_to_unique:
            self.original_to_unique[original_name] = []
        
        if len(self.original_to_unique[original_name]) == 0:
            new_name = original_name
        else:
            new_name = f"{original_name}_{len(self.original_to_unique[original_name])}"
        
        self.original_to_unique[original_name].append(new_name)
        
        if new_name != original_name:
            self.logger.warning(f"  ⚠️ Обнаружен дубликат таблицы '{original_name}'. Переименовываю в '{new_name}' ")
        
        return new_name

    def get_unique_table_name_for_insert(self, original_name: str) -> str:
        """Возвращает уникальное имя таблицы для INSERT операции на основе порядка INSERT"""
        
        if original_name not in self.original_to_unique:
            # Если таблица не найдена, возвращаем оригинальное имя
            self.logger.warning(f"  ⚠️ Таблица '{original_name}' не найдена в CREATE TABLE, оставляем как есть ")
            return original_name
        
        # Инициализируем счетчик для этого имени
        if original_name not in self.insert_counter:
            self.insert_counter[original_name] = 0
        
        # Получаем текущий индекс
        current_index = self.insert_counter[original_name]
        
        # Проверяем, что индекс не выходит за пределы списка
        if current_index >= len(self.original_to_unique[original_name]):

            # Если вышли за пределы, используем последнюю таблицу из current_index
            self.logger.debug(f"  ⚠️ Чтение многострочного INSERT таблицы '{original_name}' ")
            return self.original_to_unique[original_name][-1]
        
        # Получаем уникальное имя для текущего индекса
        unique_name = self.original_to_unique[original_name][current_index]
        
        # Увеличиваем счетчик для следующего INSERT
        self.insert_counter[original_name] += 1
        
        # Логируем маппинг для отладки
        if len(self.original_to_unique[original_name]) > 1:
            self.logger.debug(f"  🔄 Маппинг INSERT #{current_index} для '{original_name}' -> '{unique_name}' ")
        
        return unique_name

    def parse_sql_dump_simple(self, dump_content: str) -> Dict[str, List]:
        """Парсинг SQL дампа"""
        
        create_tables = []
        inserts = []
        
        # Разбиваем по строкам
        lines = dump_content.replace('\r\n', '\n').replace('\r', '\n').split('\n')
        
        current_statement = []
        
        for line in lines:
            line = line.strip()
            if not line:
                if current_statement:

                    # Завершаем текущее выражение
                    full_statement = ' '.join(current_statement).strip()
                    if full_statement:
                        if full_statement.upper().startswith('CREATE TABLE'):
                            create_tables.append(full_statement)
                        elif full_statement.upper().startswith('INSERT INTO'):
                            inserts.append(full_statement)
                    current_statement = []
                continue
            
            # Если строка начинается с ключевых слов - начинаем новое выражение
            line_upper = line.upper()
            if (line_upper.startswith('CREATE TABLE') or 
                line_upper.startswith('INSERT INTO') or
                line_upper.startswith('DROP TABLE') or
                line_upper.startswith('SET FOREIGN_KEY_CHECKS')):
                
                if current_statement:
                    full_statement = ' '.join(current_statement).strip()
                    if full_statement:
                        if full_statement.upper().startswith('CREATE TABLE'):
                            create_tables.append(full_statement)
                        elif full_statement.upper().startswith('INSERT INTO'):
                            inserts.append(full_statement)
                
                current_statement = [line]
            elif current_statement:
                current_statement.append(line)
        
        # Добавляем последнее выражение
        if current_statement:
            full_statement = ' '.join(current_statement).strip()
            if full_statement:
                if full_statement.upper().startswith('CREATE TABLE'):
                    create_tables.append(full_statement)
                elif full_statement.upper().startswith('INSERT INTO'):
                    inserts.append(full_statement)
        
        self.logger.info(f"📊 Найдено CREATE TABLE: {len(create_tables)} ")
        self.logger.info(f"📊 Найдено INSERT: {len(inserts)} ")
        
        return {
            'create_tables': create_tables,
            'inserts': inserts
        }

    def parse_create_table_simplified(self, statement: str) -> Optional[Dict]:
        """Парсинг CREATE TABLE с поддержкой дублирующихся имен"""
        
        # Извлекаем имя таблицы
        table_match = re.search(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`"\']?(\w+)[`"\']?', statement, re.IGNORECASE)
        if not table_match:
            return None
        
        original_name = table_match.group(1)
        
        # Генерируем уникальное имя таблицы
        table_name = self.get_unique_table_name(original_name)
        
        # Извлекаем содержимое между скобками
        start_idx = statement.find('(')
        end_idx = statement.rfind(')')
        
        if start_idx == -1 or end_idx == -1:
            return None
        
        columns_def = statement[start_idx + 1:end_idx]
        
        # Разбиваем на части с учетом вложенных скобок
        column_parts = []
        current_part = []
        paren_level = 0
        in_quotes = False
        quote_char = None
        in_backticks = False
        
        for char in columns_def:
            # Обработка кавычек и бэктиков
            if char == '`':
                in_backticks = not in_backticks
            elif char in ['"', "'"]:
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif char == quote_char:
                    in_quotes = False
                    quote_char = None
            
            if not in_quotes and not in_backticks:
                if char == '(':
                    paren_level += 1
                elif char == ')':
                    paren_level -= 1
                elif char == ',' and paren_level == 0:
                    column_parts.append(''.join(current_part).strip())
                    current_part = []
                    continue
            
            current_part.append(char)
        
        if current_part:
            column_parts.append(''.join(current_part).strip())
                
        # Парсим каждую часть
        columns = []
        primary_key = None
        
        for part in column_parts:
            if not part.strip():
                continue
                
            part_upper = part.upper().strip()
            
            # Обработка PRIMARY KEY constraint
            if part_upper.startswith('PRIMARY KEY'):
                pk_match = re.search(r'PRIMARY\s+KEY\s*\(([^)]+)\)', part, re.IGNORECASE)
                if pk_match:
                    pk_cols = pk_match.group(1).strip()
                    primary_key = [c.strip('` "\'') for c in pk_cols.split(',')]
                continue
            
            # Обработка UNIQUE KEY constraint
            if part_upper.startswith('UNIQUE KEY'):
                continue
            
            # Обработка FOREIGN KEY
            if part_upper.startswith('FOREIGN KEY'):
                continue
            
            # Обработка INDEX
            if part_upper.startswith('KEY') or part_upper.startswith('INDEX'):
                continue
            
            # Обработка CONSTRAINT
            if part_upper.startswith('CONSTRAINT'):
                continue
            
            # Проверка: является ли эта часть определением колонки?
            first_word = re.match(r'^[`"\']?[a-zA-Z_][a-zA-Z0-9_]*[`"\']?', part)
            if not first_word:
                continue
            
            # Парсим колонку
            col_info = self._parse_column_simplified(part)
            if col_info:
                columns.append(col_info)
            else:
                self.logger.warning(f"  ⚠️ Не удалось распарсить колонку: {part[:100]} ")
        
        if not columns:
            self.logger.error(f"  ❌ Не найдено колонок в таблице {table_name} ")
            return None
        
        # Если PRIMARY KEY не найден как constraint, ищем в определении колонок
        if not primary_key:
            for col in columns:
                if col.get('primary_key'):
                    primary_key = [col['name']]
                    break
        
        self.table_structures[table_name] = {
            'name': table_name,
            'original_name': original_name,
            'columns': columns,
            'primary_key': primary_key,
            'column_names': [col['name'] for col in columns],
            'column_types': {col['name']: col['type'] for col in columns}
        }
        
        self.logger.info(f"  ✅ Распарсена {table_name} таблица: {len(columns)} колонок" + 
                        (f" (ориг. {original_name})" if original_name != table_name else ""))
        
        return self.table_structures[table_name]
    
    def _parse_column_simplified(self, column_def: str) -> Optional[Dict]:
        """Парсинг определения колонки"""
        
        column_def = column_def.strip()
        if not column_def:
            return None
        
        # Убираем комментарии в конце
        column_def = re.sub(r'\s+--.*$', '', column_def).strip()
        
        # Извлекаем имя колонки (поддерживает: `name`, [name], name)
        name_match = re.match(r'^[`\[]?([a-zA-Z_][a-zA-Z0-9_]*)[`\]]?\s+', column_def)
        if not name_match:
            return None
        
        column_name = name_match.group(1)
        rest = column_def[len(name_match.group(0)):].strip()
        
        # Проверяем PRIMARY KEY в определении колонки
        is_primary_key = False
        if re.search(r'\bPRIMARY\s+KEY\b', rest, re.IGNORECASE):
            is_primary_key = True
            rest = re.sub(r'\bPRIMARY\s+KEY\b', '', rest, flags=re.IGNORECASE).strip()
        
        # Проверяем NOT NULL
        nullable = True
        if re.search(r'\bNOT\s+NULL\b', rest, re.IGNORECASE):
            nullable = False
            rest = re.sub(r'\bNOT\s+NULL\b', '', rest, flags=re.IGNORECASE).strip()
        
        # Проверяем AUTO_INCREMENT
        is_auto_increment = False
        if re.search(r'\bAUTO_INCREMENT\b', rest, re.IGNORECASE):
            is_auto_increment = True
            rest = re.sub(r'\bAUTO_INCREMENT\b', '', rest, flags=re.IGNORECASE).strip()
        
        # Определяем тип данных
        type_match = re.match(r'^([a-z]+)(?:\((\d+)(?:,(\d+))?\))?', rest.lower())
        if not type_match:
            return None
        
        base_type = type_match.group(1)
        width = type_match.group(2)
        scale = type_match.group(3)
        
        # Конвертируем тип используя type_mapping
        base_type_lower = base_type.lower()
        
        # Особые случаи с размером
        if base_type_lower == 'varchar' and width:
            width_int = int(width)
            if width_int > 4000:
                mssql_type = 'NVARCHAR(MAX)'
            else:
                mssql_type = f'NVARCHAR({width_int})'
        elif base_type_lower == 'char' and width:
            mssql_type = f'NCHAR({width})'
        elif base_type_lower == 'decimal' and width:
            if scale:
                mssql_type = f'DECIMAL({width},{scale})'
            else:
                mssql_type = f'DECIMAL({width},0)'
        else:
            mssql_type = self.type_mapping.get(base_type_lower, 'NVARCHAR(255)')
        
        return {
            'name': column_name,
            'type': mssql_type,
            'nullable': nullable,
            'auto_increment': is_auto_increment,
            'primary_key': is_primary_key,
            'default': None
        }
    
    def parse_inserts_simple(self, statement: str) -> List[Dict]:
        """Парсинг INSERT с поддержкой дублирующихся таблиц"""
        
        table_match = re.search(r'INSERT\s+INTO\s+`?(\w+)`?', statement, re.IGNORECASE)
        if not table_match:
            return []
        
        original_name = table_match.group(1)
        
        # Получаем уникальное имя для INSERT (уже с учетом порядка)
        table_name = self.get_unique_table_name_for_insert(original_name)
        
        rows = []
        
        # Находим VALUES
        values_pos = statement.upper().find('VALUES')
        if values_pos == -1:
            return []
        
        values_part = statement[values_pos + 6:].strip()
        if values_part.endswith(';'):
            values_part = values_part[:-1]
        
        # Парсим строки с учетом вложенных скобок и кавычек
        i = 0
        length = len(values_part)
        
        while i < length:
            # Пропускаем пробелы
            while i < length and values_part[i] in [' ', '\n', '\r', '\t']:
                i += 1
            
            if i >= length:
                break
                
            if values_part[i] == '(':
                j = i + 1
                paren_count = 1
                in_string = False
                string_char = None
                
                while j < length and paren_count > 0:
                    char = values_part[j]
                    
                    # Обработка экранирования
                    if char == '\\' and j + 1 < length:
                        j += 2
                        continue
                    
                    # Обработка кавычек
                    if char in ['"', "'"]:
                        if not in_string:
                            in_string = True
                            string_char = char
                        elif char == string_char:
                            in_string = False
                            string_char = None
                    
                    if not in_string:
                        if char == '(':
                            paren_count += 1
                        elif char == ')':
                            paren_count -= 1
                    
                    j += 1
                
                if paren_count == 0:

                    # Извлекаем строку между скобками
                    row_str = values_part[i + 1:j - 1]
                    values = self._parse_row_values(row_str)
                    rows.append({
                        'table': table_name,
                        'original_table': original_name,
                        'values': values
                    })
                    i = j
                else:
                    i += 1
            else:
                i += 1
        
        return rows
            
    def _parse_row_values(self, row_str: str, row_num: int = None, total_rows: int = None) -> List[str]:
        """Парсит значения одной строки - с поддержкой экранирования"""
        
        values = []
        current_value = []
        i = 0
        length = len(row_str)
        in_quotes = False
        quote_char = None 
        
        while i < length:
            char = row_str[i]
            
            # Обработка экранирования
            if char == '\\' and i + 1 < length:
                current_value.append(row_str[i + 1])
                i += 2
                continue
            
            # Обработка кавычек
            if char in ['"', "'"]:
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                    current_value.append(char)
                elif char == quote_char:
                    in_quotes = False
                    quote_char = None
                    current_value.append(char)
                else:
                    current_value.append(char)
                i += 1
                continue
            
            # Разделитель (только вне кавычек)
            if not in_quotes and char == ',':
                val = ''.join(current_value).strip()
                values.append(val if val else 'NULL')
                current_value = []
                i += 1
                continue
            
            current_value.append(char)
            i += 1
        
        # Последнее значение
        if current_value:
            val = ''.join(current_value).strip()
            values.append(val if val else 'NULL')
        
        return values
        
    def generate_create_table_sql(self, table_name: str) -> Optional[str]:
        """Генерирует SQL для создания таблицы"""
        
        if table_name not in self.table_structures:
            return None
        
        struct = self.table_structures[table_name]
        columns_sql = []
        
        for col in struct['columns']:
            col_sql = f"    [{col['name']}] {col['type']}"
            if not col.get('nullable', True):
                col_sql += " NOT NULL"
            columns_sql.append(col_sql)
        
        escaped_table_name = f"[{table_name}]"
        return f"CREATE TABLE {escaped_table_name} (\n{',\n'.join(columns_sql)}\n);"
    
    def generate_insert_sql_simple(self, rows: List[Dict]) -> List[str]:
        """Генерирует SQL для вставки данных"""
        
        if not rows:
            return []
        
        table_name = rows[0]['table']
        struct = self.table_structures.get(table_name)
        
        if not struct:
            self.logger.warning(f"⚠️ Структура для {table_name} не найдена ")
            return []
        
        expected_count = len(struct['column_names'])
        escaped_table_name = f"[{table_name}]"
        columns = ', '.join([f"[{col}]" for col in struct['column_names']])
        
        # Разбиваем на батчи
        batches = [rows[i:i + self.batch_size] for i in range(0, len(rows), self.batch_size)]
        result_sql = []
        
        for batch in batches:
            values_list = []
            
            for row in batch:
                row_values = row['values']
                
                # Выравниваем количество значений
                if len(row_values) != expected_count:
                    if len(row_values) > expected_count:
                        row_values = row_values[:expected_count]
                    else:
                        row_values.extend(['NULL'] * (expected_count - len(row_values)))
                
                cleaned_values = []
                for i, (col_name, val) in enumerate(zip(struct['column_names'], row_values)):
                    if not val or val.upper() == 'NULL':
                        cleaned_values.append('NULL')
                    elif val.upper() == 'CURRENT_TIMESTAMP':
                        cleaned_values.append('CURRENT_TIMESTAMP')
                    else:
                        
                        # Очищаем значение от кавычек
                        clean_val = val.strip()
                        
                        # Убираем внешние кавычки
                        if len(clean_val) >= 2:
                            if (clean_val[0] == "'" and clean_val[-1] == "'") or \
                               (clean_val[0] == '"' and clean_val[-1] == '"'):
                                clean_val = clean_val[1:-1]
                        
                        # Экранируем одиночные кавычки
                        clean_val = clean_val.replace("'", "''")
                        
                        # Обработка дат
                        if clean_val == '0000-00-00 00:00:00':
                            cleaned_values.append("'1900-01-01 00:00:00'")
                        elif re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', clean_val):
                            cleaned_values.append(f"'{clean_val}'")
                        else:
                            cleaned_values.append(f"'{clean_val}'")
                
                values_list.append(f"({', '.join(cleaned_values)})")
            
            if values_list:
                insert_sql = f"INSERT INTO {escaped_table_name} ({columns}) VALUES\n  {',\n  '.join(values_list)};"
                result_sql.append(insert_sql)
        
        return result_sql
    
    def execute_sql(self, cursor, sql: str) -> bool:
        """Выполняет SQL запрос"""
        
        try:
            cursor.execute(sql)
            cursor.commit()
            return True
        except Exception as e:
            self.logger.error(f"❌ Ошибка: {e} ")
            self.logger.debug(f"SQL: {sql[:200]}... ")
            cursor.rollback()
            return False
        
    def insert_row_by_row(self, cursor, table_name: str, rows: List[Dict], struct: Dict):
        """Построчная вставка с пропуском ошибок"""
        
        columns = ', '.join([self.escape_identifier(col) for col in struct['column_names']])
        placeholders = ', '.join(['?' for _ in struct['column_names']])
        sql = f"INSERT INTO {self.escape_identifier(table_name)} ({columns}) VALUES ({placeholders})"
        
        # Подготавливаем данные
        insert_data = []
        for row in rows:
            values = row['values']
            if len(values) != len(struct['column_names']):
                if len(values) > len(struct['column_names']):
                    values = values[:len(struct['column_names'])]
                else:
                    values.extend(['NULL'] * (len(struct['column_names']) - len(values)))
            
            cleaned = []
            for val in values:
                if not val or val.upper() == 'NULL' or val == '':
                    cleaned.append(None)
                else:
                    clean_val = val.strip()
                    if len(clean_val) >= 2:
                        if (clean_val[0] == "'" and clean_val[-1] == "'") or \
                        (clean_val[0] == '"' and clean_val[-1] == '"'):
                            clean_val = clean_val[1:-1]
                    clean_val = clean_val.replace("\\'", "'").replace('\\"', '"')
                    if clean_val == '0000-00-00 00:00:00':
                        clean_val = '0001-01-01 00:00:00'
                    cleaned.append(clean_val)
            insert_data.append(cleaned)
        
        success_count = 0
        error_count = 0
        
        for i, row_data in enumerate(insert_data):
            try:
                cursor.execute(sql, row_data)
                cursor.commit()
                success_count += 1
                if success_count % 1000 == 0:
                    self.logger.info(f"    Прогресс: {success_count}/{len(rows)}, таблица {table_name}, {error_count} ошибок ")
            except Exception as row_error:
                error_count += 1
                if error_count <= 5:
                    self.logger.warning(f"    Строка {i}: {str(row_error)[:200]} ")
                elif error_count == 6:
                    self.logger.warning(f"    ... и еще ошибки ... ")
                cursor.rollback()
        
        self.logger.info(f"  ✅ Вставлено {success_count} строк, пропущено {error_count} строк ")
        return success_count > 0
            
    def process_dump(self, dump_file_path: str):
        """Основной метод обработки дампа"""
        
        self.logger.info("=" * 60)
        self.logger.info("🚀 НАЧАЛО КОНВЕРТАЦИИ ")
        self.logger.info("=" * 60)
        
        if not os.path.exists(dump_file_path):
            self.logger.error(f"❌ Файл не найден: {dump_file_path} ")
            return
        
        # Определяем кодировку и читаем файл
        encoding = self.detect_encoding(dump_file_path)
        
        try:
            with open(dump_file_path, 'r', encoding=encoding, errors='replace') as f:
                dump_content = f.read()
            
            self.logger.info(f"📄 Файл: {os.path.basename(dump_file_path)} ")
            self.logger.info(f"📊 Размер: {len(dump_content):,} байт ")
            self.logger.info(f"📊 Кодировка: {encoding} ")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка чтения: {e} ")
            return
        
        # Подключаемся к MSSQL
        try:
            conn = self.connect_to_mssql()
            cursor = conn.cursor()
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения: {e} ")
            return
        
        try:
            # Парсим дамп
            parsed = self.parse_sql_dump_simple(dump_content)
            
            if not parsed['create_tables'] and not parsed['inserts']:
                self.logger.error("❌ Не найдено SQL выражений!")
                return
            
            # Парсим CREATE TABLE
            self.logger.info("=" * 60)
            self.logger.info("📋 ШАГ 1: Парсинг CREATE TABLE ")
            self.logger.info("=" * 60)
            
            for create_stmt in parsed['create_tables']:
                try:
                    self.parse_create_table_simplified(create_stmt)
                except Exception as e:
                    self.logger.error(f"❌ Ошибка парсинга CREATE TABLE: {e} ")
                    if not self.continue_on_error:
                        raise
            
            self.logger.info(f"✅ Распарсено таблиц: {len(self.table_structures)} ")
            
            # Выводим информацию о переименованных таблицах
            duplicates_found = False
            for original, names in self.original_to_unique.items():
                if len(names) > 1:
                    duplicates_found = True
                    self.logger.info(f"  📌 Таблица '{original}' встречается {len(names)} раз(а): ")
                    for idx, name in enumerate(names):
                        self.logger.info(f"     [{idx}] {name} ")
                    self.logger.info(f"     INSERT будут направляться в том же порядке ")

            if not duplicates_found:
                self.logger.info(f"  ✅ Дубликатов таблиц не обнаружено ")

            # Собираем INSERT данные
            self.logger.info("=" * 60)
            self.logger.info("📋 ШАГ 2: Парсинг INSERT ")
            self.logger.info("=" * 60)
            
            table_inserts = {}
            total_inserts = 0
            total_inserts_statements = len(parsed['inserts'])
                        
            for idx, insert_stmt in enumerate(parsed['inserts']):
                try:
                    rows = self.parse_inserts_simple(insert_stmt)
                    total_inserts += len(rows)
                    
                    for row in rows:
                        table_name = row['table']
                        if table_name not in table_inserts:
                            table_inserts[table_name] = []
                        table_inserts[table_name].append(row)
                    
                    if (idx + 1) % 1000 == 0:
                        progress = (idx + 1) / total_inserts_statements * 100
                        self.logger.info(f"Чтение всех insert выражений {idx + 1}/{total_inserts_statements} ({progress:.1f}%) ")
                        
                except Exception as e:
                    self.logger.error(f"❌ Ошибка парсинга INSERT #{idx}: {e} ")
                    if not self.continue_on_error:
                        raise
            
            self.logger.info(f"✅ Найдено INSERT для {len(table_inserts)} таблиц, всего строк: {total_inserts}")
            for table_name, rows in table_inserts.items():
                self.logger.info(f"  📊 {table_name}: {len(rows)} строк")
            
            # Создаем таблицы
            self.logger.info("=" * 60)
            self.logger.info("📋 ШАГ 3: Создание таблиц ")
            self.logger.info("=" * 60)
            
            for table_name in table_inserts.keys():
                if table_name not in self.table_structures:
                    self.logger.warning(f"⚠️ Нет структуры для {table_name}, пропускаем ")
                    continue
                
                self.drop_table_if_exists(table_name, cursor)
                create_sql = self.generate_create_table_sql(table_name)
                
                if create_sql and self.execute_sql(cursor, create_sql):
                    self.logger.info(f"  ✅ Таблица {table_name} создана ")
                else:
                    self.logger.error(f"  ❌ Ошибка создания {table_name} ")
            
            # Вставляем данные
            self.logger.info("=" * 60)
            self.logger.info("📋 ШАГ 4: Вставка данных ")
            self.logger.info("=" * 60)
            
            for table_name, rows in table_inserts.items():
                if table_name not in self.table_structures:
                    continue
                
                self.logger.info(f"📊 Вставка {len(rows)} строк в {table_name} ")
                struct = self.table_structures[table_name]
                
                if self.insert_row_by_row(cursor, table_name, rows, struct):
                    self.logger.info(f"  ✅ Данные вставлены в {table_name} ")
                else:
                    self.logger.error(f"  ❌ Ошибка вставки в {table_name} ")
                    if not self.continue_on_error:
                        raise
            
            self.logger.info("=" * 60)
            self.logger.info("✅ КОНВЕРТАЦИЯ ЗАВЕРШЕНА ")
            self.logger.info("=" * 60)
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка: {e} ")
            import traceback
            traceback.print_exc()
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    config = {
        'server': 'OB',
        'database': 'Dumps_4'
    }
    
    converter = MariaDBToMSSQLConverter(**config)
    converter.process_dump('E:/DataBackups/dump.sql')
