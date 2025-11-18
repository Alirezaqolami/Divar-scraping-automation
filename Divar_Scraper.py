# -*- coding: utf-8 -*-
"""
Divar Full Scraper - نسخه هوشمند شده با AI
- بهینه‌سازی هوشمند استراتژی‌های اسکرپینگ
- یادگیری تطبیقی از نتایج
- مدیریت خودکار خطاها
- اضافه: اتو-سیو مرحله‌ای (checkpoint) و resume خودکار
- بهینه‌شده برای Docker
"""
from __future__ import annotations

import os
import re
import csv
import time
import json
import random
import traceback
import logging
from typing import List, Dict, Optional, Set, Any, Tuple
from collections import OrderedDict
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.remote.remote_connection import LOGGER as SELENIUM_LOGGER

import socket  # 👈 اضافه شده برای چک اینترنت

# کاهش لاگ‌های Selenium
SELENIUM_LOGGER.setLevel(logging.WARNING)


def wait_for_internet(host="8.8.8.8", port=53, timeout=5, retry_delay=10):
    """ تا وقتی اینترنت وصل بشه صبر میکنه """
    while True:
        try:
            socket.setdefaulttimeout(timeout)
            socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
            return
        except Exception:
            log("❌ اینترنت قطع است، منتظر اتصال...")
            time.sleep(retry_delay)


# ------------------------------------------------------------------
# تنظیمات
USE_WEBDRIVER_MANAGER = True
LOCAL_CHROMEDRIVER_PATH = ""

# تنظیمات کاربر
CITY_SLUG = "shiraz"
CATEGORY_NAME = "فروش مسکونی"
CATEGORY_URL = f"https://divar.ir/s/{CITY_SLUG}/buy-residential"

OUTPUT_XLSX = "divar_sales_ai.xlsx"
SEEN_LINKS_CSV = "seen_links_ai.csv"
SEEN_LINKS_JSON = "seen_links_ai.json"
AI_LEARNING_FILE = "ai_learning_data.json"
CHECKPOINT_FILE = "checkpoint_ai.json"  # فایل checkpoint

# رفتار اسکرول/تأخیرها - بهینه‌سازی شده برای سرور
IMPLICIT_WAIT = 1
PAGE_LOAD_SLEEP = (0.5, 1.0)
LIST_SCROLL_SLEEP = (0.4, 0.8)
SCROLL_MAX_ROUNDS = 350
SCROLL_PATIENCE = 7
SCROLL_EXTRA_AFTER_STABLE = 2
DETAIL_DWELL = (0.5, 1.0)
CLICK_VIEW_MORE_SLEEP = (1.0, 1.5)
BETWEEN_ADS_SLEEP = (0.3, 0.8)

# امکانات ستونی (برچسب نمایش -> نام ستون)
FEATURES_MAP = {
    "آسانسور": "elevator",
    "پارکینگ": "parking",
    "انباری": "storage_room",
    "بالکن": "balcony",
    "جنس کف سرامیک": "floor_material_ceramic",
    "سرویس بهداشتی ایرانی": "iranian_wc",
    "سرمایش کولر آبی": "cooling_evaporative",
    "گرمایش شوفاژ": "heating_radiator",
    "تأمین‌کننده آب گرم پکیج": "hot_water_package",
}

# ستون‌های پایه جدید + ستون تاریخ ایجاد
BASE_COLUMNS = [
    "category", "لینک", "عنوان", "تاریخ", "مکان",
    "متراژ", "سال ساخت", "تعداد اتاق", "تعداد واحد در طبقه",
    "نوع سند", "وضعیت واحد", "جهت ساختمان",
    "قیمت کل", "قیمت هر متر", "طبقه",
    "جنس کف", "نوع سرویس بهداشتی", "نوع سرمایش", "نوع گرمایش", "تامین کننده آب گرم",
    "ویژگی‌ها و امکانات", "توضیحات", "تاریخ ایجاد"  # ستون جدید اضافه شد
]
FINAL_COLUMNS = BASE_COLUMNS + list(FEATURES_MAP.values())


def get_current_timestamp() -> str:
    """دریافت تاریخ و ساعت فعلی برای ستون تاریخ ایجاد"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def clean_numeric_fields(data: Dict[str, str]) -> Dict[str, str]:
    """
    پاکسازی فیلدهای عددی و تبدیل به عدد
    """
    numeric_fields = [
        'قیمت کل', 'قیمت هر متر', 'متراژ', 'سال ساخت',
        'تعداد اتاق', 'تعداد واحد در طبقه', 'طبقه'
    ]

    for field in numeric_fields:
        if field in data:
            value = data[field]

            # اگر نامشخص بود، null قرار بده
            if value in ['نامشخص', '']:
                data[field] = None
                continue

            # حذف متن‌های غیرعددی
            cleaned_value = re.sub(r'[^\d]', '', str(value))

            # اگر بعد از پاکسازی چیزی نماند، null قرار بده
            if not cleaned_value:
                data[field] = None
            else:
                # تبدیل به عدد
                try:
                    data[field] = int(cleaned_value)
                except ValueError:
                    data[field] = None

    return data


def map_feature_columns(label_list: List[str]) -> Dict[str, str]:
    """
    تبدیل لیست ویژگی‌ها به ستون‌های جداگانه
    """
    out = {}
    if not label_list:
        # اگر لیست خالی بود، همه رو ندارد قرار بده
        for col in FEATURES_MAP.values():
            out[col] = "ندارد"
        return out

    s = set(label_list)
    for fa, col in FEATURES_MAP.items():
        # فقط اگر دقیقاً مطابقت داشت، دارد قرار بده
        out[col] = "دارد" if any(fa in x for x in s) else "ندارد"
    return out


def find_value_by_title(soup: BeautifulSoup, title_text: str) -> str:
    """
    پیدا کردن مقدار بر اساس عنوان در کل صفحه
    """
    try:
        # جستجو در تمام المان‌ها
        all_elements = soup.find_all(["p", "div", "span"])

        for element in all_elements:
            text = element.get_text(strip=True)
            if title_text in text:
                # سعی کن مقدار رو از المان بعدی یا هم سطح پیدا کنی
                next_element = element.find_next()
                if next_element and next_element != element:
                    next_text = next_element.get_text(strip=True)
                    if next_text and title_text not in next_text:
                        return next_text

                # یا از parent و siblings
                parent = element.parent
                if parent:
                    siblings = parent.find_all(["p", "div", "span"])
                    for sibling in siblings:
                        sibling_text = sibling.get_text(strip=True)
                        if sibling != element and sibling_text and title_text not in sibling_text:
                            return sibling_text

                return "نامشخص"

        return "نامشخص"

    except Exception:
        return "نامشخص"


def extract_specific_details(soup: BeautifulSoup, data: Dict[str, str]) -> None:
    """
    استخراج اطلاعات خاص از المان‌های با کلاس مشخص - نسخه دقیق
    """
    try:
        log("🔍 در حال استخراج اطلاعات خاص...")

        # استخراج دقیق از کلاس‌های kt-unexpandable-row
        rows = soup.find_all("div", class_=re.compile(r"kt-base-row|kt-unexpandable-row"))

        for row in rows:
            try:
                title_element = row.find("p", class_=re.compile(r"kt-base-row__title|kt-unexpandable-row__title"))
                value_element = row.find("p", class_=re.compile(r"kt-unexpandable-row__value|value"))

                if title_element and value_element:
                    title_text = title_element.get_text(strip=True)
                    value_text = value_element.get_text(strip=True)

                    if "تعداد واحد در طبقه" in title_text:
                        # پاکسازی عددی
                        cleaned_value = re.sub(r'[^\d]', '', value_text)
                        data["تعداد واحد در طبقه"] = int(cleaned_value) if cleaned_value else None

                    elif "نوع سند" in title_text or "سند" == title_text.strip():
                        data["نوع سند"] = value_text if value_text != "نامشخص" else None

                    elif "وضعیت واحد" in title_text:
                        data["وضعیت واحد"] = value_text if value_text != "نامشخص" else None

                    elif "جهت ساختمان" in title_text or "جهت ساختمان" == title_text.strip():
                        data["جهت ساختمان"] = value_text if value_text != "نامشخص" else None

                    elif "قیمت کل" in title_text:
                        # پاکسازی عددی
                        cleaned_value = re.sub(r'[^\d]', '', value_text)
                        data["قیمت کل"] = int(cleaned_value) if cleaned_value else None

                    elif "قیمت هر متر" in title_text:
                        # پاکسازی عددی
                        cleaned_value = re.sub(r'[^\d]', '', value_text)
                        data["قیمت هر متر"] = int(cleaned_value) if cleaned_value else None

                    elif "طبقه" in title_text:
                        # پاکسازی عددی
                        cleaned_value = re.sub(r'[^\d]', '', value_text)
                        data["طبقه"] = int(cleaned_value) if cleaned_value else None

            except:
                continue

        if data.get("جهت ساختمان") in [None, "نامشخص", ""]:
            try:
                direction_title = soup.find("p", class_="kt-base-row__title", string="جهت ساختمان")
                if direction_title:
                    direction_value = direction_title.find_next_sibling("p", class_="kt-unexpandable-row__value")
                    if direction_value:
                        data["جهت ساختمان"] = direction_value.get_text(strip=True)
                        log(f"✅ جهت ساختمان مستقیم پیدا شد: {data['جهت ساختمان']}")
            except Exception as e:
                log(f"⚠️ خطا در پیدا کردن جهت ساختمان: {e}")

        # استخراج متراژ، سال ساخت، تعداد اتاق
        try:
            info_rows = soup.select("tr.kt-group-row__data-row")
            for row in info_rows:
                cells = row.select("td.kt-group-row-item--info-row, td.kt-group-row-item.kt-group-row-item__value")
                if cells:
                    vals = [c.get_text(" ", strip=True) for c in cells]
                    if len(vals) >= 3:
                        # پاکسازی مقادیر عددی
                        meterage_clean = re.sub(r'[^\d]', '', vals[0])
                        year_clean = re.sub(r'[^\d]', '', vals[1])
                        rooms_clean = re.sub(r'[^\d]', '', vals[2])

                        data["متراژ"] = int(meterage_clean) if meterage_clean else None
                        data["سال ساخت"] = int(year_clean) if year_clean else None
                        data["تعداد اتاق"] = int(rooms_clean) if rooms_clean else None
                        break
        except:
            pass

        # استخراج ویژگی‌ها از بخش kt-feature-row
        feature_elements = soup.find_all("div", class_=re.compile(r"kt-feature-row"))
        all_features = []

        for feature in feature_elements:
            try:
                title_element = feature.find("p", class_=re.compile(r"kt-feature-row__title"))
                if title_element:
                    feature_text = title_element.get_text(strip=True)
                    all_features.append(feature_text)

                    # مستقیماً مقدار رو در فیلدهای مربوطه قرار بده
                    if any(x in feature_text for x in ["جنس کف", "کف", "سرامیک", "موزاییک", "سنگ"]):
                        data["جنس کف"] = feature_text if feature_text != "نامشخص" else None
                    elif any(x in feature_text for x in ["سرویس بهداشتی", "دستشویی", "توالت", "حمام"]):
                        data["نوع سرویس بهداشتی"] = feature_text if feature_text != "نامشخص" else None
                    elif any(x in feature_text for x in ["سرمایش", "کولر", "تهویه", "هواساز"]):
                        data["نوع سرمایش"] = feature_text if feature_text != "نامشخص" else None
                    elif any(x in feature_text for x in ["گرمایش", "شوفاژ", "بخاری", "رادیاتور"]):
                        data["نوع گرمایش"] = feature_text if feature_text != "نامشخص" else None
                    elif any(x in feature_text for x in ["آب گرم", "پکیج", "منبع", "موتورخانه"]):
                        data["تامین کننده آب گرم"] = feature_text if feature_text != "نامشخص" else None

            except:
                continue

        # مستقیماً مقادیر رو از ویژگی‌ها استخراج کن
        for feature in all_features:
            if "آسانسور" in feature:
                data["elevator"] = "دارد"
            if "پارکینگ" in feature:
                data["parking"] = "دارد"
            if "انباری" in feature:
                data["storage_room"] = "دارد"
            if "بالکن" in feature:
                data["balcony"] = "دارد"
            if "جنس کف سرامیک" in feature:
                data["floor_material_ceramic"] = "دارد"
                data["جنس کف"] = "سرامیک"
            if "سرویس بهداشتی ایرانی" in feature:
                data["iranian_wc"] = "دارد"
                data["نوع سرویس بهداشتی"] = "ایرانی"
            if "سرمایش کولر آبی" in feature:
                data["cooling_evaporative"] = "دارد"
                data["نوع سرمایش"] = "کولر آبی"
            if "گرمایش شوفاژ" in feature:
                data["heating_radiator"] = "دارد"
                data["نوع گرمایش"] = "شوفاژ"
            if "تأمین‌کننده آب گرم پکیج" in feature:
                data["hot_water_package"] = "دارد"
                data["تامین کننده آب گرم"] = "پکیج"

        # برای فیلدهایی که هنوز پر نشدن، مقدار پیش‌فرض قرار بده
        text_fields_defaults = {
            "جنس کف": None,
            "نوع سرویس بهداشتی": None,
            "نوع سرمایش": None,
            "نوع گرمایش": None,
            "تامین کننده آب گرم": None
        }

        for field, default_val in text_fields_defaults.items():
            if data.get(field) in [None, "نامشخص", ""]:
                data[field] = default_val

        # ویژگی‌ها و امکانات
        data["ویژگی‌ها و امکانات"] = "، ".join(all_features) if all_features else None

        # برای امکاناتی که مقدار ندارن، ندارد قرار بده
        for col in FEATURES_MAP.values():
            if col not in data:
                data[col] = "ندارد"

        log(f"✅ اطلاعات استخراج شده: {len(all_features)} ویژگی")

    except Exception as e:
        log(f"❌ خطا در استخراج اطلاعات خاص: {e}")
        # مقادیر پیش‌فرض برای همه فیلدها
        default_fields = {
            "تعداد واحد در طبقه": None,
            "نوع سند": None,
            "وضعیت واحد": None,
            "جهت ساختمان": None,
            "قیمت کل": None,
            "قیمت هر متر": None,
            "طبقه": None,
            "متراژ": None,
            "سال ساخت": None,
            "تعداد اتاق": None,
            "جنس کف": None,
            "نوع سرویس بهداشتی": None,
            "نوع سرمایش": None,
            "نوع گرمایش": None,
            "تامین کننده آب گرم": None,
            "ویژگی‌ها و امکانات": None
        }
        data.update(default_fields)

        for col in FEATURES_MAP.values():
            data[col] = "ندارد"


def find_in_text(soup: BeautifulSoup, primary_term: str, secondary_term: str) -> str:
    """
    جستجوی یک عبارت در متن صفحه
    """
    try:
        # جستجو در تمام المان‌ها
        for element in soup.find_all(["p", "div", "span"]):
            text = element.get_text(strip=True)
            if primary_term in text or secondary_term in text:
                # سعی کنیم مقدار رو از المان مجاور پیدا کنیم
                parent = element.parent
                if parent:
                    siblings = parent.find_all(["p", "div", "span"])
                    for sibling in siblings:
                        if sibling != element and sibling.get_text(strip=True):
                            return sibling.get_text(strip=True)

                # یا از متن خود المان استفاده کنیم
                return text

        return "نامشخص"
    except:
        return "نامشخص"


# ----------------------------- ابزارهای کمکی -----------------------------
# تنظیمات لاگ‌گیری
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('divar_scraper_ai.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)


def log(msg: str, level: str = "INFO") -> None:
    log_level = getattr(logging, level.upper())
    logging.log(log_level, msg)


def human_sleep(a: float, b: float) -> None:
    time.sleep(random.uniform(a, b))


def ensure_dir_for_file(path: str) -> None:
    d = os.path.dirname(os.path.abspath(path))
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def read_seen_links_csv(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()
    s = set()
    with open(path, "r", encoding="utf-8") as f:
        rdr = csv.reader(f)
        for row in rdr:
            if row:
                s.add(row[0].strip())
    return s


def append_seen_links_csv(path: str, links: List[str]) -> None:
    if not links:
        return
    ensure_dir_for_file(path)
    with open(path, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for lk in links:
            w.writerow([lk])


def read_seen_links_json(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return set(data) if isinstance(data, list) else set()
    except Exception:
        return set()


def write_seen_links_json(path: str, links: Set[str]) -> None:
    ensure_dir_for_file(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(list(links), f, ensure_ascii=False, indent=2)


def load_existing_links_from_excel(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()
    try:
        df = pd.read_excel(path)
        if "لینک" in df.columns:
            return set(df["لینک"].astype(str).str.strip().tolist())
    except Exception:
        pass
    return set()


# ----------------------------- checkpoint helpers -----------------------------
def atomic_write_json(path: str, data: Any) -> None:
    """نوشتن ایمن JSON (atomic)"""
    ensure_dir_for_file(path)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    try:
        os.replace(tmp, path)
    except Exception:
        try:
            os.remove(path)
            os.replace(tmp, path)
        except Exception:
            pass


def load_checkpoint(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data
    except Exception as e:
        log(f"⚠️ خطا در بارگذاری checkpoint: {e}")
        return None


def save_checkpoint(path: str, state: Dict[str, Any]) -> None:
    """ذخیره وضعیت فعلی (پس از هر آگهی)"""
    try:
        atomic_write_json(path, state)
        log(f"💾 checkpoint ذخیره شد: {path} (processed: {len(state.get('processed_links', []))})")
    except Exception as e:
        log(f"⚠️ خطا در ذخیره checkpoint: {e}")


def clear_checkpoint(path: str) -> None:
    try:
        if os.path.exists(path):
            os.remove(path)
            log("🧹 checkpoint پاک شد.")
    except Exception as e:
        log(f"⚠️ خطا در پاک‌کردن checkpoint: {e}")


# ----------------------------- کلاس بهینه‌ساز AI -----------------------------
class AIScrapingOptimizer:
    def __init__(self):
        self.scraping_patterns = []
        self.error_patterns = []
        self.success_rates = {}
        self.learning_data = self._load_learning_data()

    def _load_learning_data(self) -> List[Dict]:
        if os.path.exists(AI_LEARNING_FILE):
            try:
                with open(AI_LEARNING_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return []
        return []

    def _save_learning_data(self):
        ensure_dir_for_file(AI_LEARNING_FILE)
        with open(AI_LEARNING_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.learning_data, f, ensure_ascii=False, indent=2)

    def analyze_page_structure(self, driver, page_type="list") -> Dict[str, Any]:
        """تحلیل هوشمند ساختار صفحه برای تعیین بهترین استراتژی اسکرپ"""
        try:
            key_elements = self._identify_key_elements(driver, page_type)

            # دیباگ: نمایش المان‌های پیدا شده
            log(f"تجزیه صفحه {page_type}: {key_elements}")

            optimal_strategy = self._determine_optimal_strategy(key_elements, page_type)

            return {
                "strategy": optimal_strategy,
                "key_elements": key_elements,
                "confidence_score": self._calculate_confidence(key_elements)
            }

        except Exception as e:
            log(f"خطا در تحلیل ساختار صفحه: {e}")
            return self._get_fallback_strategy(page_type)

    def _identify_key_elements(self, driver, page_type) -> Dict[str, Any]:
        """شناسایی هوشمند المان‌های مهم صفحه"""
        elements = {}

        try:
            if page_type == "list":
                # شناسایی کارت‌های آگهی
                ad_candidates = driver.find_elements(By.XPATH,
                                                     "//article | //div[contains(@class, 'card')] | //div[contains(@class, 'post')]")
                elements['ad_containers'] = len(ad_candidates)

                # شناسایی دکمه‌های pagination
                pagination_elements = driver.find_elements(By.XPATH,
                                                           "//a[contains(@href, 'page')] | //button[contains(text(), 'بعدی')]")
                elements['has_pagination'] = len(pagination_elements) > 0

                # شناسایی اسکرول infinit
                scroll_height = driver.execute_script("return document.body.scrollHeight")
                viewport_height = driver.execute_script("return window.innerHeight")
                elements['is_infinite_scroll'] = scroll_height > viewport_height * 3

            elif page_type == "detail":
                # شناسایی بخش‌های اطلاعاتی
                info_sections = driver.find_elements(By.XPATH, "//div[contains(@class, 'info')] | //table | //dl")
                elements['info_sections'] = len(info_sections)

                # شناسایی دکمه نمایش بیشتر
                show_more_buttons = driver.find_elements(By.XPATH,
                                                         "//button[contains(text(), 'نمایش')] | //a[contains(text(), 'نمایش')]")
                elements['has_show_more'] = len(show_more_buttons) > 0

        except Exception as e:
            log(f"خطا در شناسایی المان‌ها: {e}")

        return elements

    def _determine_optimal_strategy(self, elements, page_type) -> Dict[str, Any]:
        """تعیین بهترین استراتژی بر اساس المان‌های شناسایی شده"""
        if page_type == "list":
            if elements.get('is_infinite_scroll', False):
                return {
                    "type": "infinite_scroll",
                    "scroll_increment": 800,
                    "scroll_delay": (0.8, 1.2),
                    "max_attempts": 15
                }
            elif elements.get('has_pagination', False):
                return {
                    "type": "pagination",
                    "page_load_delay": (1.5, 2.0)
                }
            else:
                return {
                    "type": "standard_scroll",
                    "scroll_increment": 600,
                    "scroll_delay": (1.0, 1.5)
                }

        elif page_type == "detail":
            if elements.get('has_show_more', False):
                return {
                    "type": "click_show_more",
                    "wait_after_click": (2.0, 3.0)
                }
            else:
                return {
                    "type": "direct_extraction",
                    "extraction_delay": (1.0, 1.8)
                }

    def _get_fallback_strategy(self, page_type) -> Dict[str, Any]:
        """استراتژی fallback در صورت خطا"""
        if page_type == "list":
            return {
                "type": "standard_scroll",
                "scroll_increment": 600,
                "scroll_delay": (1.0, 1.5)
            }
        else:
            return {
                "type": "direct_extraction",
                "extraction_delay": (1.0, 1.8)
            }

    def _calculate_confidence(self, elements) -> float:
        """محاسبه میزان اطمینان از تحلیل"""
        if not elements:
            return 0.5

        confidence = 0.5
        if elements.get('ad_containers', 0) > 0:
            confidence += 0.2
        if elements.get('info_sections', 0) > 0:
            confidence += 0.2

        return min(confidence, 1.0)

    def optimize_extraction_selectors(self, soup, current_data) -> Dict[str, str]:
        """بهینه‌سازی سلکتورهای استخراج داده بر اساس محتوای صفحه"""
        optimized_selectors = {}

        for field in ['متراژ', 'قیمت کل', 'تعداد اتاق', 'سال ساخت']:
            best_selector = self._find_best_selector_for_field(soup, field, current_data.get(field, ""))
            if best_selector:
                optimized_selectors[field] = best_selector

        return optimized_selectors

    def _find_best_selector_for_field(self, soup, field_name, current_value) -> Optional[str]:
        """یافتن بهترین سلکتور برای هر فیلد"""
        patterns = [
            f"//*[contains(text(), '{field_name}')]/following-sibling::*",
            f"//*[contains(@class, '{field_name.lower()}')]",
            f"//*[contains(text(), '{field_name.split()[0]}')]",
        ]

        for pattern in patterns:
            try:
                elements = soup.select(pattern) if pattern.startswith('.') else soup.find_all(pattern)
                if elements and any(self._is_valid_value(elem.get_text(), field_name) for elem in elements):
                    return pattern
            except:
                continue

        return None

    def _is_valid_value(self, value, field_name) -> bool:
        """اعتبارسنجی هوشمند مقادیر استخراج شده"""
        value = value.strip()
        if not value or value == "نامشخص":
            return False

        if field_name == "متراژ" and "متر" in value:
            return True
        if field_name == "قیمت کل" and ("تومان" in value or "ریال" in value):
            return True
        if field_name == "تعداد اتاق" and any(char.isdigit() for char in value):
            return True

        return len(value) > 1

    def learn_from_results(self, url, strategy_used, success_rate, extracted_data) -> None:
        """یادگیری از نتایج برای بهبود استراتژی‌های آینده"""
        learning_entry = {
            "url": url,
            "strategy": strategy_used,
            "success_rate": success_rate,
            "timestamp": time.time(),
            "data_quality": self._calculate_data_quality(extracted_data)
        }

        self.learning_data.append(learning_entry)
        self._save_learning_data()

        if success_rate > 0.8:
            self.scraping_patterns.append(strategy_used)

        self.scraping_patterns = [pattern for pattern in self.scraping_patterns
                                  if self._get_pattern_success_rate(pattern) > 0.6]

    def _calculate_data_quality(self, data) -> float:
        """محاسبه کیفیت داده‌های استخراج شده"""
        if not data:
            return 0.0

        required_fields = ['عنوان', 'متراژ', 'قیمت کل']
        quality_score = 0.0

        for field in required_fields:
            if field in data and data[field] not in [None, "", "نامشخص"]:
                quality_score += 0.3

        return min(quality_score, 1.0)

    def get_recommended_strategy(self, page_type) -> Dict[str, Any]:
        """دریافت بهترین استراتژی بر اساس یادگیری قبلی"""
        if not self.scraping_patterns:
            return self._get_fallback_strategy(page_type)

        best_pattern = max(self.scraping_patterns,
                           key=lambda x: self._get_pattern_success_rate(x))

        return best_pattern

    def _get_pattern_success_rate(self, pattern) -> float:
        """محاسبه نرخ موفقیت یک الگو"""
        relevant_entries = [entry for entry in self.learning_data
                            if entry['strategy']['type'] == pattern['type']]

        if not relevant_entries:
            return 0.5

        return sum(entry['success_rate'] for entry in relevant_entries) / len(relevant_entries)


# ----------------------------- درایور (بهینه‌شده برای Docker) -----------------------------
def build_driver(headless: bool = True) -> webdriver.Chrome:
    """
    ساخت و پیکربندی درایور Chrome - نسخه پایدار برای Docker
    """
    import os
    import tempfile
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options

    # ایجاد مسیر cache امن با دسترسی کامل
    cache_dir = "/tmp/wdm_cache"
    os.makedirs(cache_dir, exist_ok=True)
    os.chmod(cache_dir, 0o777)

    # تنظیم محیط برای webdriver-manager
    os.environ['WDM_LOG_LEVEL'] = '0'
    os.environ['WDM_LOCAL'] = '1'
    os.environ['WDM_CACHE_PATH'] = cache_dir

    opts = Options()
    opts.page_load_strategy = 'eager'  # لود سریع صفحه

    # تنظیم مسیر Chrome - فقط اگر وجود دارد
    chrome_path = "/usr/bin/google-chrome"
    if os.path.exists(chrome_path):
        opts.binary_location = chrome_path
        log(f"✅ استفاده از Chrome: {chrome_path}")
    else:
        log("⚠️ Chrome یافت نشد، استفاده از chromedriver داخلی")

    # تنظیمات اجباری برای Docker
    opts.add_argument("--blink-settings=imagesEnabled=false")  # غیرفعال کردن تصاویر
    opts.add_argument("--disable-http2")
    opts.add_argument("--disable-quic")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-ipc-flooding-protection")
    opts.add_argument("--disable-client-side-phishing-detection")
    opts.add_argument("--disable-component-extensions-with-background-pages")
    opts.add_argument("--disable-default-apps")
    opts.add_argument("--disable-plugins")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--disable-prompt-on-repost")
    opts.add_argument("--disable-sync")
    opts.add_argument("--safebrowsing-disable-auto-update")
    opts.add_argument("--metrics-recording-only")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")
    opts.add_argument("--media-cache-size=1")
    opts.add_argument("--disk-cache-size=1")
    opts.add_argument("--aggressive-cache-discard")

    if headless:
        opts.add_argument("--headless=new")

    # تنظیمات user-agent و زبان
    ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    opts.add_argument(f"--user-agent={ua}")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--lang=fa-IR")

    # تنظیمات experimental
    opts.add_experimental_option("prefs", {
        "profile.default_content_setting_values.notifications": 2,
        "profile.managed_default_content_settings.images": 2,  # غیرفعال کردن تصاویر
        "profile.default_content_settings.popups": 0,
        "profile.default_content_settings.geolocation": 2,
        "profile.default_content_settings.media_stream": 2,  # غیرفعال کردن ویدیو/صدا
        "profile.default_content_settings.cookies": 2,
        "profile.default_content_settings.plugins": 2,
        "profile.default_content_settings.mixed_script": 2,
        "profile.default_content_settings.media_stream": 2,
    })

    max_retries = 3
    for attempt in range(max_retries):
        try:
            log(f"🔄 تلاش {attempt + 1}/{max_retries} برای راه‌اندازی درایور...")

            # روش 1: استفاده از webdriver-manager با مدیریت خطا
            try:
                from webdriver_manager.chrome import ChromeDriverManager
                service = Service(ChromeDriverManager(cache_path=cache_dir).install())
            except Exception as e:
                log(f"⚠️ webdriver-manager خطا خورد: {e}")
                # روش 2: استفاده از chromedriver از سیستم
                service = Service("/usr/local/bin/chromedriver")

            driver = webdriver.Chrome(service=service, options=opts)

            # تنظیمات timeout
            driver.set_page_load_timeout(30)
            driver.set_script_timeout(20)
            driver.implicitly_wait(10)

            # مخفی کردن automation
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            # تست سلامت درایور
            driver.get("about:blank")
            current_url = driver.current_url
            if "about:blank" in current_url:
                log("✅ درایور با موفقیت راه‌اندازی و تست شد")
                return driver
            else:
                raise Exception("تست سلامت درایور ناموفق بود")

        except Exception as e:
            log(f"❌ خطا در تلاش {attempt + 1}: {str(e)}")

            if attempt == max_retries - 1:
                log("🔥 استفاده از راهکار نهایی...")
                return _ultimate_fallback_driver(headless)

            import time
            time.sleep(2)


def _ultimate_fallback_driver(headless: bool = True) -> webdriver.Chrome:
    """
    راهکار نهایی برای زمانی که همه روش‌ها شکست می‌خورند
    """
    try:
        log("🚨 استفاده از راهکار نهایی (تنظیما مینیمال)...")

        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service

        opts = Options()
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")

        if headless:
            opts.add_argument("--headless=new")

        # سعی کن chromedriver را مستقیماً پیدا کن
        possible_paths = [
            "/usr/local/bin/chromedriver",
            "/usr/bin/chromedriver",
            "/app/chromedriver",
            "chromedriver"  # استفاده از PATH
        ]

        for path in possible_paths:
            try:
                service = Service(executable_path=path)
                driver = webdriver.Chrome(service=service, options=opts)
                log(f"✅ درایور با مسیر {path} راه‌اندازی شد")
                return driver
            except:
                continue

        # آخرین تلاش: بدون service
        driver = webdriver.Chrome(options=opts)
        log("✅ درایور بدون service راه‌اندازی شد")
        return driver

    except Exception as e:
        log(f"💥 خطای نهایی در راه‌اندازی درایور: {e}")
        raise Exception(f"امکان راه‌اندازی درایور وجود ندارد: {e}")


def check_system_dependencies():
    """
    بررسی وابستگی‌های سیستم قبل از اجرا - نسخه بهبود یافته
    """
    import os
    import subprocess
    import shutil

    log("🔍 بررسی وابستگی‌های سیستم...")

    # بررسی Python و pip
    try:
        python_version = subprocess.run(["python3", "--version"], capture_output=True, text=True)
        log(f"✅ Python: {python_version.stdout.strip()}")
    except Exception as e:
        log(f"❌ Python بررسی نشد: {e}")

    # بررسی مرورگرها با اولویت
    browsers = [
        ("google-chrome", "/usr/bin/google-chrome"),
        ("chromium-browser", "/usr/bin/chromium-browser"),
        ("chromium", "/usr/bin/chromium")
    ]

    available_browsers = []
    for name, path in browsers:
        if os.path.exists(path):
            available_browsers.append((name, path))
            try:
                version = subprocess.run([path, "--version"], capture_output=True, text=True, timeout=5)
                log(f"✅ {name}: {version.stdout.strip()}")
            except subprocess.TimeoutExpired:
                log(f"⚠️ {name}: timeout در بررسی نسخه")
            except Exception as e:
                log(f"⚠️ {name}: خطا در بررسی نسخه - {e}")
        else:
            log(f"❌ {name}: یافت نشد")

    # بررسی ابزارهای ضروری
    tools = ["unzip", "curl", "wget"]
    for tool in tools:
        if shutil.which(tool):
            log(f"✅ {tool}: یافت شد")
        else:
            log(f"⚠️ {tool}: یافت نشد")

    # بررسی دسترسی‌های دایرکتوری
    test_dirs = ["/tmp", "/app", "/home"]
    for test_dir in test_dirs:
        if os.path.exists(test_dir):
            try:
                test_file = os.path.join(test_dir, "test_write")
                with open(test_file, "w") as f:
                    f.write("test")
                os.remove(test_file)
                log(f"✅ دسترسی نوشتن در {test_dir}: مجاز")
            except Exception as e:
                log(f"❌ دسترسی نوشتن در {test_dir}: ممنوع - {e}")

    success = len(available_browsers) > 0
    if success:
        log("✅ سیستم برای اجرا آماده است")
    else:
        log("❌ هیچ مرورگری یافت نشد! اجرا ممکن است با مشکل مواجه شود")

    return success


def test_driver_connection():
    """
    تست اتصال درایور با مدیریت خطای بهتر
    """
    try:
        log("🧪 تست اتصال درایور...")

        # تست سریع بدون لاگ‌گیری اضافی
        driver = build_driver(headless=True)

        # تست باز کردن صفحه
        driver.get("https://www.google.com")
        title = driver.title
        log(f"✅ تست اتصال موفق: {title}")

        driver.quit()
        return True

    except Exception as e:
        log(f"❌ تست اتصال ناموفق: {e}")
        return False


# ----------------------------- استخراج لینک‌ها (هوشمند) -----------------------------
def close_map_if_exists(driver: webdriver.Chrome) -> None:
    """بستن نقشه شناور (FAB)"""
    try:
        candidates = driver.find_elements(By.CSS_SELECTOR, "div.kt-fab-button, div[class*='kt-fab-button']")
        candidates += driver.find_elements(By.XPATH, "//div[contains(@class,'kt-fab-button')]")
        for el in candidates:
            try:
                ActionChains(driver).move_to_element(el).pause(0.05).click(el).perform()
                log("نقشه بسته شد.")
                human_sleep(1, 1.5)
                return
            except Exception:
                continue
    except Exception:
        pass


def get_ad_links_ai(category_url: str, category_name: str, ai_optimizer: AIScrapingOptimizer) -> List[str]:
    """
    اسکرول هوشمند با استفاده از تحلیل AI برای استخراج لینک‌ها
    """
    driver = build_driver(headless=True)  # headless=True برای سرور
    try:
        log(f"ورود به: {category_url}")
        wait_for_internet()
        driver.get(category_url)

        # تحلیل ساختار صفحه توسط AI
        page_analysis = ai_optimizer.analyze_page_structure(driver, "list")
        strategy = page_analysis["strategy"]
        log(f"استراتژی انتخاب شده: {strategy['type']} (اعتماد: {page_analysis['confidence_score']:.2f})")

        close_map_if_exists(driver)

        seen_ordered: List[str] = []
        seen_set: Set[str] = set()
        last_unique_count = 0
        no_new_rounds = 0
        max_rounds = SCROLL_MAX_ROUNDS

        if strategy["type"] == "infinite_scroll":
            max_rounds = strategy.get("max_attempts", SCROLL_MAX_ROUNDS)

        for round_idx in range(1, max_rounds + 1):
            anchors = driver.find_elements(By.CSS_SELECTOR,
                                           "article.kt-post-card a[href], a.kt-post-card__action[href], article a[href]")
            dom_cards = driver.find_elements(By.CSS_SELECTOR, "article.kt-post-card")
            dom_count = len(dom_cards)

            for a in anchors:
                try:
                    href = a.get_attribute("href") or ""
                except Exception:
                    href = ""
                href = href.strip()
                if not href:
                    continue
                if href.startswith("/"):
                    href = "https://divar.ir" + href
                if "/v/" not in href:
                    continue
                if href not in seen_set:
                    seen_set.add(href)
                    seen_ordered.append(href)

            log(f"[round {round_idx}] DOM_cards={dom_count} | unique_links={len(seen_ordered)}")

            # اعمال استراتژی اسکرول بر اساس تحلیل AI
            try:
                if strategy["type"] == "infinite_scroll":
                    scroll_amount = strategy.get("scroll_increment", 800)
                    driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                    human_sleep(*strategy.get("scroll_delay", LIST_SCROLL_SLEEP))
                elif strategy["type"] == "standard_scroll":
                    if dom_cards:
                        driver.execute_script("arguments[0].scrollIntoView({block: 'end'});", dom_cards[-1])
                    else:
                        scroll_amount = strategy.get("scroll_increment", 600)
                        driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                    human_sleep(*strategy.get("scroll_delay", LIST_SCROLL_SLEEP))
            except Exception:
                driver.execute_script("window.scrollBy(0, window.innerHeight);")
                human_sleep(*LIST_SCROLL_SLEEP)

            if len(seen_ordered) == last_unique_count:
                no_new_rounds += 1
            else:
                no_new_rounds = 0
                last_unique_count = len(seen_ordered)

            if no_new_rounds >= SCROLL_PATIENCE:
                log(f"توقف: {no_new_rounds} دور پیاپی لینک جدید نیامد (patience={SCROLL_PATIENCE}).")
                for _ in range(SCROLL_EXTRA_AFTER_STABLE):
                    driver.execute_script("window.scrollBy(0, 2000);")
                    human_sleep(*LIST_SCROLL_SLEEP)
                break

        # استخراج نهایی لینک‌ها
        human_sleep(0.9, 1.3)
        anchors = driver.find_elements(By.CSS_SELECTOR,
                                       "article.kt-post-card a[href], a.kt-post-card__action[href], article a[href]")
        for a in anchors:
            try:
                href = a.get_attribute("href") or ""
            except:
                href = ""
            href = href.strip()
            if not href:
                continue
            if href.startswith("/"):
                href = "https://divar.ir" + href
            if "/v/" not in href:
                continue
            if href not in seen_set:
                seen_set.add(href)
                seen_ordered.append(href)

        log(f"تعداد لینک‌های نهایی: {len(seen_ordered)}")

        # یادگیری از نتایج
        success_rate = min(len(seen_ordered) / 50, 1.0)  # نرخ موفقیت تقریبی
        ai_optimizer.learn_from_results(category_url, strategy, success_rate, {"links_count": len(seen_ordered)})

        return seen_ordered

    finally:
        try:
            driver.quit()
        except Exception:
            pass


def click_show_all_details(driver: webdriver.Chrome) -> bool:
    """
    تلاش برای کلیک روی «نمایش همهٔ جزئیات» - نسخه بسیار ساده
    """
    try:
        log("🔍 در حال جستجوی دکمه 'نمایش همهٔ جزئیات'...")

        # اول صفحه رو خوب اسکرول کنیم
        driver.execute_script("window.scrollBy(0, 800);")
        human_sleep(0.1, 0.9)
        driver.execute_script("window.scrollBy(0, 400);")
        human_sleep(0.1, 0.8)

        # 💡 روش 1: ساده‌ترین روش - جستجوی مستقیم
        try:
            # پیدا کردن المان با متن دقیق
            show_more_element = driver.find_element(By.XPATH, "//*[text()='نمایش همهٔ جزئیات']")
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", show_more_element)
            human_sleep(0.1, 1)
            driver.execute_script("arguments[0].click();", show_more_element)
            log("✅ کلیک موفقیت‌آمیز با متن دقیق")
            human_sleep(0.4, 1.1)
            return True
        except:
            pass

        # 💡 روش 2: جستجوی با contains
        try:
            show_more_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'نمایش همه')]")
            for element in show_more_elements:
                try:
                    if element.is_displayed():
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                        human_sleep(0.5, 1)
                        driver.execute_script("arguments[0].click();", element)
                        log("✅ کلیک موفقیت‌آمیز با contains")
                        human_sleep(1.0, 2.0)
                        return True
                except:
                    continue
        except:
            pass

        # 💡 روش 3: جستجو با JavaScript
        try:
            result = driver.execute_script("""
                // پیدا کردن همه المان‌ها
                var allElements = document.querySelectorAll('*');
                for (var i = 0; i < allElements.length; i++) {
                    var element = allElements[i];
                    var text = element.textContent || element.innerText || '';

                    // اگر متن شامل 'نمایش همه' باشد
                    if (text.includes('نمایش همهٔ جزئیات') || text.includes('نمایش همه')) {
                        // اسکرول و کلیک
                        element.scrollIntoView({behavior: 'smooth', block: 'center'});
                        element.click();
                        return true;
                    }
                }
                return false;
            """)

            if result:
                log("✅ کلیک با JavaScript موفقیت‌آمیز بود")
                human_sleep(1.0, 2.0)
                return True
        except Exception as js_error:
            log(f"⚠️ خطا در JavaScript: {js_error}")

        log("⚠️ دکمه 'نمایش همه جزئیات' پیدا نشد. ممکن است صفحه از قبل گسترش یافته باشد.")
        return False

    except Exception as e:
        log(f"⚠️ خطا در کلیک نمایش جزئیات: {e}")
        return False


def extract_value_by_title(soup: BeautifulSoup, title_text: str, default: str = "نامشخص") -> str:
    """
    استخراج مقدار بر اساس عنوان
    """
    try:
        # جستجوی مستقیم با متن عنوان
        title_elements = soup.find_all(["p", "span", "div"], string=re.compile(f".*{re.escape(title_text)}.*"))

        for title_el in title_elements:
            # بررسی المان‌های هم level در کنار عنوان
            parent = title_el.find_parent()
            if parent:
                # جستجوی مقدار در المان‌های مجاور
                value_elements = parent.find_all(["p", "span", "div"],
                                                 class_=re.compile("value|end|value-box|amount|number"))

                for value_el in value_elements:
                    if value_el != title_el and value_el.get_text(strip=True):
                        raw_value = value_el.get_text(strip=True)

                        # اگر فیلد عددی هست، پاکسازی کن
                        if title_text in ['قیمت کل', 'قیمت هر متر', 'طبقه']:
                            cleaned_value = re.sub(r'[^\d]', '', raw_value)
                            return cleaned_value if cleaned_value else "نامشخص"
                        else:
                            return raw_value

                # جستجو در sibling elements
                next_sibling = title_el.find_next_sibling()
                if next_sibling and next_sibling.get_text(strip=True):
                    raw_value = next_sibling.get_text(strip=True)
                    if title_text in ['قیمت کل', 'قیمت هر متر', 'طبقه']:
                        cleaned_value = re.sub(r'[^\d]', '', raw_value)
                        return cleaned_value if cleaned_value else "نامشخص"
                    else:
                        return raw_value

        return default

    except Exception:
        return default


def scrape_ad_detail(driver: webdriver.Chrome, link: str, category: str) -> Optional[Dict[str, str]]:
    """
    باز کردن صفحه آگهی، کلیک نمایش جزییات، استخراج جزئیات و امکانات
    """
    try:
        wait_for_internet()
        driver.get(link)
        human_sleep(*DETAIL_DWELL)

        # بستن pop-up های احتمالی
        try:
            popup_selectors = [
                "button[aria-label='بستن']",
                "div[class*='close']",
                "button[class*='close']",
                "svg[class*='close']"
            ]
            for selector in popup_selectors:
                try:
                    close_btn = driver.find_element(By.CSS_SELECTOR, selector)
                    close_btn.click()
                    human_sleep(0.3, 0.7)
                except:
                    continue
        except:
            pass

        # 💡 مهم: قبل از کلیک اسکرول کنیم
        driver.execute_script("window.scrollBy(0, 500);")
        human_sleep(1.0, 1.5)

        # 💡 اول صفحه رو پردازش کنیم (قبل از کلیک)
        soup_before_click = BeautifulSoup(driver.page_source, "html.parser")

        # تلاش برای باز کردن جزئیات بیشتر
        clicked = click_show_all_details(driver)

        if clicked:
            log("✅ کلیک موفق، منتظر لود جزئیات...")
            human_sleep(2.0, 3.0)  # زمان بیشتر برای لود جزئیات

            # 💡 بعد از کلیک، صفحه رو دوباره پردازش کنیم
            soup_after_click = BeautifulSoup(driver.page_source, "html.parser")

            # از صفحه بعد از کلیک استفاده کنیم
            soup = soup_after_click
        else:
            log("⚠️ کلیک انجام نشد، ادامه با اطلاعات فعلی")
            human_sleep(1.0, 1.5)
            # از صفحه قبل از کلیک استفاده کنیم
            soup = soup_before_click

        # اسکرول مجدد برای اطمینان
        driver.execute_script("window.scrollBy(0, 300);")
        human_sleep(0.8, 1.2)

        data: Dict[str, str] = {"category": category, "لینک": link}

        # عنوان
        title_el = soup.select_one("h1.kt-page-title__title")
        data["عنوان"] = title_el.get_text(" ", strip=True) if title_el else None

        # تاریخ/مکان
        sub = soup.select_one("div.kt-page-title__subtitle")
        if sub:
            txt = sub.get_text(" ", strip=True)
            m = re.match(r"(.+?)\s+در\s+(.+)", txt)
            if m:
                data["تاریخ"] = m.group(1).strip() if m.group(1).strip() != "نامشخص" else None
                data["مکان"] = m.group(2).strip() if m.group(2).strip() != "نامشخص" else None
            else:
                data["تاریخ"] = None
                data["مکان"] = txt if txt != "نامشخص" else None
        else:
            data["تاریخ"], data["مکان"] = None, None

        # استخراج اطلاعات خاص از المان‌های با کلاس مشخص
        extract_specific_details(soup, data)

        # متراژ/سال ساخت/تعداد اتاق
        data["متراژ"] = data["سال ساخت"] = data["تعداد اتاق"] = None
        try:
            rows = soup.select("tr.kt-group-row__data-row")
            for row in rows:
                cells = row.select("td.kt-group-row-item--info-row, td.kt-group-row-item.kt-group-row-item__value")
                if not cells:
                    continue
                vals = [c.get_text(" ", strip=True) for c in cells]
                if len(vals) >= 3:
                    # پاکسازی مقادیر عددی
                    meterage_clean = re.sub(r'[^\d]', '', vals[0])
                    year_clean = re.sub(r'[^\d]', '', vals[1])
                    rooms_clean = re.sub(r'[^\d]', '', vals[2])

                    data["متراژ"] = int(meterage_clean) if meterage_clean else None
                    data["سال ساخت"] = int(year_clean) if year_clean else None
                    data["تعداد اتاق"] = int(rooms_clean) if rooms_clean else None
                    break
                elif len(vals) == 2:
                    meterage_clean = re.sub(r'[^\d]', '', vals[0])
                    year_clean = re.sub(r'[^\d]', '', vals[1])
                    data["متراژ"] = int(meterage_clean) if meterage_clean else None
                    data["سال ساخت"] = int(year_clean) if year_clean else None
                    break
                elif len(vals) == 1:
                    meterage_clean = re.sub(r'[^\d]', '', vals[0])
                    data["متراژ"] = int(meterage_clean) if meterage_clean else None
                    break
        except Exception:
            pass

        # قیمت‌ها
        price_total = extract_value_by_title(soup, "قیمت کل", "نامشخص")
        price_per_meter = extract_value_by_title(soup, "قیمت هر متر", "نامشخص")
        floor = extract_value_by_title(soup, "طبقه", "نامشخص")

        # پاکسازی مقادیر عددی
        data["قیمت کل"] = int(re.sub(r'[^\d]', '', price_total)) if price_total != "نامشخص" else None
        data["قیمت هر متر"] = int(re.sub(r'[^\d]', '', price_per_meter)) if price_per_meter != "نامشخص" else None
        data["طبقه"] = int(re.sub(r'[^\d]', '', floor)) if floor != "نامشخص" else None

        # امکانات رشته‌ای و ستونی
        feature_titles = [p.get_text(strip=True) for p in soup.find_all("p", class_="kt-feature-row__title")]
        data["ویژگی‌ها و امکانات"] = ", ".join(feature_titles) if feature_titles else None

        # توضیحات - بهبود یافته
        desc = soup.select_one("p.kt-description-row__text.kt-description-row__text--primary")
        if not desc:
            # جستجوی جایگزین برای توضیحات
            desc_selectors = [
                "p.kt-description-row__text",
                "div.kt-description-row__text",
                "div[class*='description']",
                "p[class*='description']"
            ]
            for selector in desc_selectors:
                desc = soup.select_one(selector)
                if desc:
                    break

        if desc:
            data["توضیحات"] = "\n".join([ln.strip() for ln in desc.get_text("\n").splitlines() if ln.strip()])
        else:
            data["توضیحات"] = None

        # پاکسازی نهایی فیلدهای عددی
        data = clean_numeric_fields(data)

        # برای فیلدهای متنی هم اگر نامشخص بود، null قرار بده
        text_fields = ['عنوان', 'مکان', 'تاریخ', 'نوع سند', 'وضعیت واحد',
                       'جهت ساختمان', 'جنس کف', 'نوع سرویس بهداشتی',
                       'نوع سرمایش', 'نوع گرمایش', 'تامین کننده آب گرم',
                       'ویژگی‌ها و امکانات', 'توضیحات']

        for field in text_fields:
            if field in data and data[field] in ['نامشخص', '']:
                data[field] = None

        # برای فیلدهای امکانات هم null قرار بده اگر ندارد باشه
        for feature_col in FEATURES_MAP.values():
            if feature_col in data and data[feature_col] == 'ندارد':
                data[feature_col] = None

        # اضافه کردن تاریخ ایجاد
        data["تاریخ ایجاد"] = get_current_timestamp()

        return data

    except Exception as e:
        log(f"خطا در خواندن جزئیات {link}: {e}")
        traceback.print_exc()
        return None


def save_to_excel(rows: List[Dict[str, str]], filename: str = OUTPUT_XLSX) -> None:
    if not rows:
        log("چیزی برای ذخیره وجود ندارد.")
        return

    # تبدیل مقادیر 'نامشخص' به None قبل از ساخت DataFrame
    for row in rows:
        for key, value in row.items():
            if value in ['نامشخص', 'ندارد', '']:
                row[key] = None

    df_new = pd.DataFrame(rows)

    for col in FINAL_COLUMNS:
        if col not in df_new.columns:
            df_new[col] = None  # به جای "نامشخص" از None استفاده کن
    df_new = df_new[FINAL_COLUMNS]

    if os.path.exists(filename):
        try:
            df_old = pd.read_excel(filename)
            # مطمئن شو که فایل قدیمی هم مقادیر نامشخص رو به None تبدیل کنه
            for col in df_old.columns:
                df_old[col] = df_old[col].replace(['نامشخص', 'ندارد', ''], None)
        except Exception:
            df_old = pd.DataFrame(columns=FINAL_COLUMNS)

        df_combined = pd.concat([df_old, df_new], ignore_index=True)
        if "لینک" in df_combined.columns:
            df_combined.drop_duplicates(subset=["لینک"], keep="last", inplace=True)
        df_combined.to_excel(filename, index=False)
    else:
        df_new.to_excel(filename, index=False)

    log(f"ذخیره شد: {filename} (ردیف‌ها: {len(pd.read_excel(filename))})")


def dedupe_links(all_links: List[str]) -> List[str]:
    seen_csv = read_seen_links_csv(SEEN_LINKS_CSV)
    seen_json = read_seen_links_json(SEEN_LINKS_JSON)
    seen_excel = load_existing_links_from_excel(OUTPUT_XLSX)
    seen = seen_csv | seen_json | seen_excel
    filtered = [lk for lk in all_links if lk not in seen]
    log(f"بعد از حذف دوپلیکیت‌ها: {len(filtered)} از {len(all_links)}")
    return filtered


def ask_how_many(max_n: int) -> int:
    # برای سرور، همه لینک‌ها پردازش شوند
    return max_n


# ----------------------------- اصلی -----------------------------
def main():
    # ابتدا بررسی وابستگی‌های سیستم
    if not check_system_dependencies():
        log("⚠️ برخی وابستگی‌ها یافت نشدند، ادامه با ریسک...")

    log(f"شروع اسکرپ هوشمند: {CATEGORY_NAME} — {CATEGORY_URL}")

    # تست اتصال درایور قبل از شروع اصلی
    try:
        log("🧪 تست اولیه اتصال درایور...")
        test_driver = build_driver(headless=True)
        test_driver.quit()
        log("✅ تست اتصال موفقیت‌آمیز بود")
    except Exception as e:
        log(f"❌ تست اتصال ناموفق: {e}")
        log("🔥 ادامه عملیات ممکن است با مشکل مواجه شود")

    # ایجاد بهینه‌ساز AI
    ai_optimizer = AIScrapingOptimizer()

    # اگر checkpoint وجود داشته باشه، از همون ادامه میدیم
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    if checkpoint:
        log("🔁 checkpoint پیدا شد — ادامه از وضعیت ذخیره‌شده.")
        # ساختار checkpoint ما: { "to_process": [...], "next_idx": int, "processed_links": [...], "scraped_rows": [...] }
        to_process = checkpoint.get("to_process", [])
        next_idx = checkpoint.get("next_idx", 1)
        scraped_rows = checkpoint.get("scraped_rows", [])
        processed_links = checkpoint.get("processed_links", [])
        # اگر to_process خالیه، ممکنه نیاز باشه لیست جدید لینک‌ها رو بگیریم.
        if not to_process:
            log("⚠️ لیست to_process در checkpoint خالی است — استخراج لینک‌ها دوباره انجام می‌شود.")

            # راه‌اندازی درایور برای استخراج لینک‌ها
            try:
                all_links = get_ad_links_ai(CATEGORY_URL, CATEGORY_NAME, ai_optimizer)
            except Exception as e:
                log(f"❌ خطا در استخراج لینک‌ها: {e}")
                return

            if not all_links:
                log("هیچ لینکی پیدا نشد.")
                return
            new_links = dedupe_links(all_links)
            if not new_links:
                log("تمام لینک‌ها از قبل دیده شده‌اند.")
                return
            n = ask_how_many(len(new_links))
            to_process = new_links[:n]
            # به‌روزرسانی checkpoint اولیه
            checkpoint_state = {
                "to_process": to_process,
                "next_idx": 1,
                "processed_links": processed_links,
                "scraped_rows": scraped_rows
            }
            save_checkpoint(CHECKPOINT_FILE, checkpoint_state)
    else:
        # حالت عادی: استخراج لینک‌ها توسط AI
        try:
            all_links = get_ad_links_ai(CATEGORY_URL, CATEGORY_NAME, ai_optimizer)
        except Exception as e:
            log(f"❌ خطا در استخراج لینک‌ها: {e}")
            return

        if not all_links:
            log("هیچ لینکی پیدا نشد.")
            return

        # حذف لینک‌های دیده‌شده
        new_links = dedupe_links(all_links)
        if not new_links:
            log("تمام لینک‌ها از قبل دیده شده‌اند.")
            return

        # برای سرور، همه لینک‌ها پردازش شوند
        n = ask_how_many(len(new_links))
        to_process = new_links[:n]
        log(f"{len(to_process)} لینک برای پردازش انتخاب شد.")

        # ایجاد checkpoint اولیه
        scraped_rows = []
        processed_links = []
        next_idx = 1
        checkpoint_state = {
            "to_process": to_process,
            "next_idx": next_idx,
            "processed_links": processed_links,
            "scraped_rows": scraped_rows
        }
        save_checkpoint(CHECKPOINT_FILE, checkpoint_state)

    # درایور دوم برای جزئیات - با مدیریت خطای پیشرفته
    try:
        detail_driver = build_driver(headless=True)  # headless=True برای سرور
        log("✅ درایور جزئیات با موفقیت راه‌اندازی شد")
    except Exception as e:
        log(f"❌ خطا در راه‌اندازی درایور جزئیات: {e}")
        log("🔥 ادامه عملیات غیرممکن است")
        return

    success_count = 0

    try:
        total = len(to_process)
        log(f"آغاز پردازش {total} لینک (شروع از idx={next_idx})")

        # iterator از idx مشخص شده
        for idx in range(next_idx, total + 1):
            link = to_process[idx - 1]
            log(f"[{idx}/{total}] پردازش: {link}")

            try:
                # بررسی سلامت درایور قبل از هر پردازش
                try:
                    detail_driver.current_url  # تست ساده اتصال
                except Exception:
                    log("⚠️ درایور قطع شده، راه‌اندازی مجدد...")
                    try:
                        detail_driver.quit()
                    except:
                        pass
                    detail_driver = build_driver(headless=True)
                    log("✅ درایور مجدداً راه‌اندازی شد")

                row = scrape_ad_detail(detail_driver, link, CATEGORY_NAME)
                if row:
                    scraped_rows.append(row)
                    processed_links.append(link)
                    success_count += 1

                    # یادگیری از نتایج موفق
                    ai_optimizer.learn_from_results(
                        link,
                        {"type": "detail_extraction"},
                        1.0,  # نرخ موفقیت
                        row
                    )
                else:
                    # یادگیری از خطاها
                    ai_optimizer.learn_from_results(
                        link,
                        {"type": "detail_extraction"},
                        0.0,  # نرخ موفقیت
                        {}
                    )
                    log("رد شد یا خطا داشت.")

            except Exception as e:
                # اگر خطای غیرمنتظره‌ای وسط پردازش پیش اومد، لاگ کن و ذخیره checkpoint سپس ادامه یا خارج شو
                log(f"⚠️ خطا هنگام پردازش لینک {link}: {e}")
                traceback.print_exc()

            # بعد از هر آگهی، checkpoint ذخیره میشه (اتو سیو مرحله‌ای)
            next_idx = idx + 1
            checkpoint_state = {
                "to_process": to_process,
                "next_idx": next_idx,
                "processed_links": processed_links,
                "scraped_rows": scraped_rows
            }
            save_checkpoint(CHECKPOINT_FILE, checkpoint_state)

            human_sleep(*BETWEEN_ADS_SLEEP)

    except Exception as e:
        log(f"❌ خطای کلی در حین پردازش: {e}")
        traceback.print_exc()
    finally:
        try:
            detail_driver.quit()
            log("✅ درایور بسته شد")
        except Exception as e:
            log(f"⚠️ خطا در بستن درایور: {e}")

    # ذخیره نتایج نهایی (اگر چیزی جمع شده)
    if scraped_rows:
        try:
            save_to_excel(scraped_rows, OUTPUT_XLSX)
            append_seen_links_csv(SEEN_LINKS_CSV, processed_links)
            write_seen_links_json(SEEN_LINKS_JSON, set(list(read_seen_links_json(SEEN_LINKS_JSON)) + processed_links))
            log(f"{len(processed_links)} لینک جدید به تاریخچه اضافه شد.")

            # گزارش نهایی به AI
            success_rate = success_count / len(to_process) if to_process else 0.0
            log(f"نرخ موفقیت استخراج: {success_rate:.2%}")

            # پس از ذخیره نهایی، checkpoint پاک میشه تا اجرای بعدی از ابتدا شروع کنه
            clear_checkpoint(CHECKPOINT_FILE)
            log("✅ checkpoint پاک شد")

        except Exception as e:
            log(f"❌ خطا در ذخیره‌سازی نهایی: {e}")
            log("⚠️ checkpoint حفظ شد تا داده‌ها از دست نروند")

    else:
        log("هیچ داده‌ای برای ذخیره نبود. checkpoint نگه داشته می‌شود تا دفعه بعد ادامه دهید.")

    log("پایان اسکرپ هوشمند.")

if __name__ == "__main__":
        main()
