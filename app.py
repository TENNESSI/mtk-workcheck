from __future__ import annotations

import calendar
import os
import re
from datetime import datetime, time, timedelta
from functools import wraps
from pathlib import Path

from flask import Flask, abort, flash, redirect, render_template, request, url_for
from flask_login import (
    LoginManager,
    UserMixin,
    current_user,
    login_required,
    login_user,
    logout_user,
)
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, inspect, or_
import pdfplumber
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "instance" / "app.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


db = SQLAlchemy()
login_manager = LoginManager()
login_manager.login_view = "login"
login_manager.login_message = "Сначала войдите в систему."


event_participants = db.Table(
    "event_participants",
    db.Column("event_id", db.Integer, db.ForeignKey("calendar_event.id"), primary_key=True),
    db.Column("user_id", db.Integer, db.ForeignKey("user.id"), primary_key=True),
)


class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    full_name = db.Column(db.String(120), nullable=False)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(20), nullable=False, default="employee")
    department = db.Column(db.String(120))
    is_active_user = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    workplace = db.relationship("Workplace", back_populates="employee", uselist=False)
    assigned_equipment = db.relationship("EquipmentItem", back_populates="assigned_user", lazy="dynamic")
    created_events = db.relationship(
        "CalendarEvent", back_populates="creator", foreign_keys="CalendarEvent.created_by_id", lazy="dynamic"
    )
    responsible_events = db.relationship(
        "CalendarEvent",
        back_populates="responsible_user",
        foreign_keys="CalendarEvent.responsible_user_id",
        lazy="dynamic",
    )
    notifications = db.relationship(
        "Notification", back_populates="user", cascade="all, delete-orphan", lazy="dynamic"
    )

    def set_password(self, password: str) -> None:
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"

    @property
    def unread_notifications_count(self) -> int:
        return self.notifications.filter_by(is_read=False).count()


class Workplace(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(120), nullable=False)
    location = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text)
    employee_id = db.Column(db.Integer, db.ForeignKey("user.id"), unique=True)

    employee = db.relationship("User", back_populates="workplace")
    equipment_items = db.relationship("EquipmentItem", back_populates="workplace", lazy="dynamic")


class EquipmentItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    category = db.Column(db.String(100), nullable=False)
    brand = db.Column(db.String(100))
    model = db.Column(db.String(150))
    asset_tag = db.Column(db.String(100), unique=True)
    serial_number = db.Column(db.String(120))
    status = db.Column(db.String(30), nullable=False, default="in_stock")
    notes = db.Column(db.Text)
    purchase_date = db.Column(db.Date)
    last_maintenance_date = db.Column(db.Date)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    assigned_user_id = db.Column(db.Integer, db.ForeignKey("user.id"))
    workplace_id = db.Column(db.Integer, db.ForeignKey("workplace.id"))

    assigned_user = db.relationship("User", back_populates="assigned_equipment")
    workplace = db.relationship("Workplace", back_populates="equipment_items")
    maintenance_logs = db.relationship(
        "MaintenanceLog", back_populates="equipment_item", cascade="all, delete-orphan", lazy="dynamic"
    )

    @property
    def display_name(self) -> str:
        parts = [self.category, self.brand, self.model]
        return " ".join([part for part in parts if part]).strip()

    @property
    def is_computer(self) -> bool:
        category = (self.category or "").lower()
        return any(word in category for word in ["комп", "систем", "pc", "computer"])

    @property
    def maintenance_is_stale(self) -> bool:
        if not self.is_computer:
            return False
        if not self.last_maintenance_date:
            return True
        return self.last_maintenance_date <= (datetime.utcnow().date() - timedelta(days=90))


class MaintenanceLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    equipment_item_id = db.Column(db.Integer, db.ForeignKey("equipment_item.id"), nullable=False)
    performed_at = db.Column(db.Date, nullable=False)
    notes = db.Column(db.Text)
    created_by_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    equipment_item = db.relationship("EquipmentItem", back_populates="maintenance_logs")
    created_by = db.relationship("User")


class CartridgeItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(150), nullable=False)
    vendor = db.Column(db.String(100))
    printer_models = db.Column(db.String(255))
    quantity_in_stock = db.Column(db.Integer, nullable=False, default=0)
    quantity_waiting_refill = db.Column(db.Integer, nullable=False, default=0)
    notes = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class CalendarEvent(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(150), nullable=False)
    description = db.Column(db.Text)
    location = db.Column(db.String(255))
    starts_at = db.Column(db.DateTime, nullable=False)
    ends_at = db.Column(db.DateTime, nullable=False)
    is_shared = db.Column(db.Boolean, nullable=False, default=False)
    created_by_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    responsible_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    creator = db.relationship("User", foreign_keys=[created_by_id], back_populates="created_events")
    responsible_user = db.relationship("User", foreign_keys=[responsible_user_id], back_populates="responsible_events")
    participants = db.relationship("User", secondary=event_participants, lazy="subquery")

    @property
    def is_personal(self) -> bool:
        return not self.is_shared

    @property
    def sorted_participants(self):
        return sorted(self.participants, key=lambda user: (user.department or "", user.full_name))

    def is_visible_to(self, user: User) -> bool:
        return bool(user.is_admin or self.is_shared or self.created_by_id == user.id)

    def can_edit(self, user: User) -> bool:
        return bool(user.is_admin or self.created_by_id == user.id)


class ExcursionItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    excursion_date = db.Column(db.Date, nullable=False, index=True)
    starts_at = db.Column(db.Time, nullable=False)
    client = db.Column(db.String(255), nullable=False)
    action = db.Column(db.String(255), nullable=False)
    guide_name = db.Column(db.String(150))
    note = db.Column(db.Text)
    source_filename = db.Column(db.String(255))
    imported_by_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    guide_user_id = db.Column(db.Integer, db.ForeignKey("user.id"))
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    imported_by = db.relationship("User", foreign_keys=[imported_by_id])
    guide_user = db.relationship("User", foreign_keys=[guide_user_id])

    @property
    def starts_at_label(self) -> str:
        return self.starts_at.strftime("%H:%M") if self.starts_at else "—"

    @property
    def guide_display_name(self) -> str:
        if self.guide_user:
            return self.guide_user.full_name
        if self.guide_name:
            return self.guide_name
        return "Не указан"


class ExcursionGuideAlias(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    alias_name = db.Column(db.String(150), nullable=False, unique=True)
    alias_normalized = db.Column(db.String(150), nullable=False, unique=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    user = db.relationship("User")


class Notification(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    title = db.Column(db.String(200), nullable=False)
    message = db.Column(db.Text, nullable=False)
    link = db.Column(db.String(255))
    is_read = db.Column(db.Boolean, default=False, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    user = db.relationship("User", back_populates="notifications")


@login_manager.user_loader
def load_user(user_id: str) -> User | None:
    return db.session.get(User, int(user_id))


def parse_date(date_string: str | None):
    if not date_string:
        return None
    return datetime.strptime(date_string, "%Y-%m-%d").date()


def parse_datetime(dt_string: str | None):
    if not dt_string:
        return None
    return datetime.strptime(dt_string, "%Y-%m-%dT%H:%M")


def safe_int(value: str | None, default: int = 0) -> int:
    try:
        return int(value or default)
    except (TypeError, ValueError):
        return default


def attach_employee_to_workplace(workplace: Workplace, employee: User | None) -> None:
    if employee is None:
        workplace.employee = None
        return
    if employee.workplace and employee.workplace.id != workplace.id:
        employee.workplace.employee = None
    workplace.employee = employee


def seed_default_admin() -> None:
    if User.query.count() > 0:
        return
    admin = User(
        full_name=os.getenv("DEFAULT_ADMIN_NAME", "Администратор"),
        username=os.getenv("DEFAULT_ADMIN_USERNAME", "admin"),
        role="admin",
        department="Администрация",
    )
    admin.set_password(os.getenv("DEFAULT_ADMIN_PASSWORD", "admin123"))
    db.session.add(admin)
    db.session.commit()


def create_notification(user: User, title: str, message: str, link: str | None = None) -> None:
    db.session.add(Notification(user=user, title=title, message=message, link=link))


def get_departments() -> list[str]:
    rows = (
        db.session.query(User.department)
        .filter(User.department.isnot(None), User.department != "")
        .order_by(User.department.asc())
        .distinct()
        .all()
    )
    return [row[0] for row in rows if row[0]]


def normalize_spaces(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def normalize_person_name(value: str | None) -> str:
    value = normalize_spaces(value).lower().replace("ё", "е")
    value = re.sub(r"[^а-яa-z0-9]+", " ", value)
    return normalize_spaces(value)


def build_name_variants(full_name: str | None) -> set[str]:
    normalized_full = normalize_person_name(full_name)
    variants = {normalized_full} if normalized_full else set()
    parts = normalized_full.split()
    if len(parts) >= 3:
        last, first, middle = parts[0], parts[1], parts[2]
        variants.add(normalize_person_name(f"{last} {first[0]} {middle[0]}"))
        variants.add(normalize_person_name(f"{last} {first[0]}.{middle[0]}."))
    elif len(parts) == 2:
        last, first = parts[0], parts[1]
        variants.add(normalize_person_name(f"{last} {first[0]}"))
        variants.add(normalize_person_name(f"{last} {first[0]}."))
    return {variant for variant in variants if variant}


def user_can_import_excursions(user: User | None) -> bool:
    if not user or not getattr(user, "is_authenticated", False):
        return False
    if user.is_admin:
        return True
    department = normalize_person_name(user.department)
    return "экскурс" in department


def match_excursion_guide_user(guide_name: str | None) -> User | None:
    normalized = normalize_person_name(guide_name)
    if not normalized:
        return None

    alias = ExcursionGuideAlias.query.filter_by(alias_normalized=normalized).first()
    if alias:
        return alias.user

    users = User.query.filter(User.is_active_user.is_(True)).all()
    matches = [user for user in users if normalized in build_name_variants(user.full_name)]
    if len(matches) == 1:
        return matches[0]
    return None


def parse_excursions_pdf(file_storage) -> tuple[datetime.date, list[dict[str, object]]]:
    parsed_date = None
    excursions: list[dict[str, object]] = []
    filename = secure_filename(file_storage.filename or "excursions.pdf") or "excursions.pdf"

    date_pattern = re.compile(r"Дата:\s*(\d{2}\.\d{2}\.\d{4})")
    file_storage.stream.seek(0)

    with pdfplumber.open(file_storage.stream) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            date_match = date_pattern.search(text)
            if date_match:
                page_date = datetime.strptime(date_match.group(1), "%d.%m.%Y").date()
                if parsed_date and parsed_date != page_date:
                    raise ValueError("В PDF найдены экскурсии на разные даты. Загружайте по одному дню за раз.")
                parsed_date = page_date

            for table in page.extract_tables() or []:
                if not table or len(table) < 2:
                    continue

                header = [normalize_spaces(cell).lower() for cell in (table[0] or [])]
                if "время" not in header or not any("клиент" in cell for cell in header) or "действие" not in header:
                    continue

                def column_index(keyword: str) -> int | None:
                    for index, cell in enumerate(header):
                        if keyword in cell:
                            return index
                    return None

                time_idx = column_index("время")
                client_idx = column_index("клиент")
                action_idx = column_index("действие")
                guide_idx = column_index("экскурсовод")
                note_idx = column_index("примеч")

                if time_idx is None or client_idx is None or action_idx is None:
                    continue

                for raw_row in table[1:]:
                    row = list(raw_row) + [""] * (len(header) - len(raw_row))
                    time_raw = normalize_spaces(row[time_idx])
                    client = normalize_spaces(row[client_idx])
                    action = normalize_spaces(row[action_idx])
                    guide_name = normalize_spaces(row[guide_idx]) if guide_idx is not None else ""
                    note = normalize_spaces(row[note_idx]) if note_idx is not None else ""

                    if not time_raw or not client or not action:
                        continue
                    if not re.fullmatch(r"\d{2}:\d{2}", time_raw):
                        continue

                    starts_at = datetime.strptime(time_raw, "%H:%M").time()
                    guide_user = match_excursion_guide_user(guide_name)
                    excursions.append(
                        {
                            "excursion_date": parsed_date,
                            "starts_at": starts_at,
                            "client": client,
                            "action": action,
                            "guide_name": guide_name,
                            "guide_user": guide_user,
                            "note": note,
                            "source_filename": filename,
                        }
                    )

    if not parsed_date:
        raise ValueError("Не удалось определить дату экскурсий из PDF.")
    if not excursions:
        raise ValueError("В PDF не найдена таблица экскурсий.")

    for excursion in excursions:
        excursion["excursion_date"] = parsed_date

    excursions.sort(key=lambda item: (item["starts_at"], item["client"], item["action"]))
    return parsed_date, excursions


def replace_excursions_for_date(excursion_date, excursions: list[dict[str, object]], imported_by: User) -> int:
    ExcursionItem.query.filter_by(excursion_date=excursion_date).delete(synchronize_session=False)
    for excursion in excursions:
        db.session.add(
            ExcursionItem(
                excursion_date=excursion_date,
                starts_at=excursion["starts_at"],
                client=excursion["client"],
                action=excursion["action"],
                guide_name=excursion["guide_name"],
                guide_user=excursion["guide_user"],
                note=excursion["note"],
                source_filename=excursion["source_filename"],
                imported_by=imported_by,
            )
        )
    return len(excursions)


def admin_required(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if not current_user.is_authenticated:
            return login_manager.unauthorized()
        if not current_user.is_admin:
            abort(403)
        return view_func(*args, **kwargs)

    return wrapped


def event_permission_or_404(event: CalendarEvent) -> CalendarEvent:
    if not event.is_visible_to(current_user):
        abort(403)
    return event


def event_edit_permission_or_404(event: CalendarEvent) -> CalendarEvent:
    if not event.can_edit(current_user):
        abort(403)
    return event


def personal_calendar_filter_for(user: User):
    return or_(
        CalendarEvent.created_by_id == user.id,
        CalendarEvent.responsible_user_id == user.id,
        event_participants.c.user_id == user.id,
    )


def ensure_schema_updates() -> None:
    inspector = inspect(db.engine)
    user_columns = {column["name"] for column in inspector.get_columns("user")}
    event_columns = {column["name"] for column in inspector.get_columns("calendar_event")}

    with db.engine.begin() as conn:
        if "department" not in user_columns:
            conn.exec_driver_sql("ALTER TABLE user ADD COLUMN department VARCHAR(120)")
        if "is_shared" not in event_columns:
            conn.exec_driver_sql("ALTER TABLE calendar_event ADD COLUMN is_shared BOOLEAN NOT NULL DEFAULT 0")
        if "responsible_user_id" not in event_columns:
            conn.exec_driver_sql("ALTER TABLE calendar_event ADD COLUMN responsible_user_id INTEGER")
        conn.exec_driver_sql(
            "UPDATE calendar_event SET responsible_user_id = created_by_id WHERE responsible_user_id IS NULL"
        )


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret-change-me")
    app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{DB_PATH}"
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    db.init_app(app)
    login_manager.init_app(app)

    register_filters(app)
    register_context(app)
    register_routes(app)

    with app.app_context():
        db.create_all()
        ensure_schema_updates()
        seed_default_admin()

    return app


def register_filters(app: Flask) -> None:
    @app.template_filter("dt")
    def format_datetime(value: datetime | None, fmt: str = "%d.%m.%Y %H:%M") -> str:
        if not value:
            return "—"
        return value.strftime(fmt)

    @app.template_filter("d")
    def format_date(value, fmt: str = "%d.%m.%Y") -> str:
        if not value:
            return "—"
        return value.strftime(fmt)



def register_context(app: Flask) -> None:
    @app.context_processor
    def inject_globals():
        unread_count = current_user.unread_notifications_count if current_user.is_authenticated else 0
        departments = get_departments() if current_user.is_authenticated else []
        return {
            "unread_notifications_count": unread_count,
            "departments": departments,
            "can_import_excursions": user_can_import_excursions(current_user) if current_user.is_authenticated else False,
            "statuses": {
                "in_stock": "На складе",
                "assigned": "Выдано",
                "repair": "В ремонте",
                "retired": "Списано",
            },
        }



def build_event_recipients(event: CalendarEvent) -> list[User]:
    if not event.is_shared:
        return []
    users_by_id = {user.id: user for user in event.participants}
    users_by_id[event.responsible_user.id] = event.responsible_user
    users_by_id[event.creator.id] = event.creator
    return [user for user in users_by_id.values() if user.id != event.creator.id]



def notify_event_created(event: CalendarEvent) -> None:
    for user in build_event_recipients(event):
        create_notification(
            user,
            "Новое мероприятие",
            f"Вас добавили в мероприятие «{event.title}» на {event.starts_at.strftime('%d.%m.%Y %H:%M')}.",
            url_for("event_detail", event_id=event.id),
        )



def notify_event_updated(event: CalendarEvent, affected_user_ids: set[int]) -> None:
    if not event.is_shared:
        return
    for user in User.query.filter(User.id.in_(affected_user_ids)).all():
        if user.id == event.creator.id:
            continue
        create_notification(
            user,
            "Мероприятие обновлено",
            f"Мероприятие «{event.title}» изменено. Новое время: {event.starts_at.strftime('%d.%m.%Y %H:%M')}.",
            url_for("event_detail", event_id=event.id),
        )



def notify_event_deleted(title: str, starts_at: datetime, user_ids: set[int]) -> None:
    if not user_ids:
        return
    for user in User.query.filter(User.id.in_(user_ids)).all():
        if user.id == current_user.id:
            continue
        create_notification(
            user,
            "Мероприятие отменено",
            f"Мероприятие «{title}», назначенное на {starts_at.strftime('%d.%m.%Y %H:%M')}, было удалено.",
            url_for("calendar_view"),
        )



def normalize_event_participants(form, creator: User, responsible_user: User | None, is_shared: bool) -> list[User]:
    if not is_shared:
        return [creator]

    participant_ids = {int(user_id) for user_id in form.getlist("participants") if user_id.isdigit()}
    participant_ids.add(creator.id)
    if responsible_user:
        participant_ids.add(responsible_user.id)
    if not participant_ids:
        participant_ids.add(creator.id)
    return User.query.filter(User.id.in_(participant_ids)).order_by(User.full_name.asc()).all()



def register_routes(app: Flask) -> None:
    @app.route("/")
    def index():
        if current_user.is_authenticated:
            return redirect(url_for("dashboard"))
        return redirect(url_for("login"))

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if current_user.is_authenticated:
            return redirect(url_for("dashboard"))
        if request.method == "POST":
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "")
            user = User.query.filter(func.lower(User.username) == username.lower()).first()
            if user and user.is_active_user and user.check_password(password):
                login_user(user)
                flash("Вы вошли в систему.", "success")
                return redirect(request.args.get("next") or url_for("dashboard"))
            flash("Неверный логин или пароль.", "danger")
        return render_template("login.html")

    @app.route("/logout")
    @login_required
    def logout():
        logout_user()
        flash("Вы вышли из системы.", "info")
        return redirect(url_for("login"))

    @app.route("/dashboard")
    @login_required
    def dashboard():
        if current_user.is_admin:
            upcoming_events = (
                CalendarEvent.query.filter(CalendarEvent.ends_at >= datetime.utcnow())
                .order_by(CalendarEvent.starts_at.asc())
                .limit(8)
                .all()
            )
            equipment = EquipmentItem.query.order_by(EquipmentItem.created_at.desc()).limit(8).all()
            stale_computers = (
                EquipmentItem.query.filter(EquipmentItem.category.isnot(None))
                .order_by(EquipmentItem.last_maintenance_date.asc().nullsfirst())
                .all()
            )
        else:
            upcoming_events = (
                CalendarEvent.query.filter(
                    CalendarEvent.ends_at >= datetime.utcnow(),
                    or_(CalendarEvent.is_shared.is_(True), CalendarEvent.created_by_id == current_user.id),
                )
                .order_by(CalendarEvent.starts_at.asc())
                .limit(8)
                .all()
            )
            equipment = current_user.assigned_equipment.order_by(EquipmentItem.category.asc()).all()
            stale_computers = []

        notifications = current_user.notifications.order_by(Notification.created_at.desc()).limit(5).all()
        workplace = current_user.workplace
        stats = {
            "users_count": User.query.count(),
            "workplaces_count": Workplace.query.count(),
            "equipment_count": EquipmentItem.query.count(),
            "cartridges_count": CartridgeItem.query.count(),
        }
        return render_template(
            "dashboard.html",
            upcoming_events=upcoming_events,
            notifications=notifications,
            workplace=workplace,
            equipment=equipment,
            stale_computers=stale_computers,
            stats=stats,
        )

    @app.route("/users")
    @admin_required
    def users_list():
        users = User.query.order_by(User.full_name.asc()).all()
        return render_template("users_list.html", users=users)

    @app.route("/users/create", methods=["GET", "POST"])
    @admin_required
    def user_create():
        if request.method == "POST":
            user = User(
                full_name=request.form.get("full_name", "").strip(),
                username=request.form.get("username", "").strip(),
                role=request.form.get("role", "employee"),
                department=request.form.get("department", "").strip(),
                is_active_user=bool(request.form.get("is_active_user")),
            )
            password = request.form.get("password", "")
            if not user.full_name or not user.username or not password:
                flash("Заполните ФИО, логин и пароль.", "danger")
            elif User.query.filter(func.lower(User.username) == user.username.lower()).first():
                flash("Пользователь с таким логином уже существует.", "danger")
            else:
                user.set_password(password)
                db.session.add(user)
                db.session.commit()
                flash("Сотрудник добавлен.", "success")
                return redirect(url_for("users_list"))
        return render_template("user_form.html", user=None)

    @app.route("/users/<int:user_id>/edit", methods=["GET", "POST"])
    @admin_required
    def user_edit(user_id: int):
        user = db.session.get(User, user_id) or abort(404)
        if request.method == "POST":
            username = request.form.get("username", "").strip()
            existing = User.query.filter(func.lower(User.username) == username.lower(), User.id != user.id).first()
            if existing:
                flash("Пользователь с таким логином уже существует.", "danger")
                return render_template("user_form.html", user=user)

            user.full_name = request.form.get("full_name", "").strip()
            user.username = username
            user.role = request.form.get("role", "employee")
            user.department = request.form.get("department", "").strip()
            user.is_active_user = bool(request.form.get("is_active_user"))
            password = request.form.get("password", "")
            if password:
                user.set_password(password)
            if not user.full_name or not user.username:
                flash("Заполните обязательные поля.", "danger")
            else:
                db.session.commit()
                flash("Данные сотрудника обновлены.", "success")
                return redirect(url_for("users_list"))
        return render_template("user_form.html", user=user)

    @app.route("/users/<int:user_id>/delete", methods=["POST"])
    @admin_required
    def user_delete(user_id: int):
        user = db.session.get(User, user_id) or abort(404)
        if user.id == current_user.id:
            flash("Нельзя удалить текущего администратора.", "danger")
            return redirect(url_for("users_list"))
        db.session.delete(user)
        db.session.commit()
        flash("Сотрудник удалён.", "info")
        return redirect(url_for("users_list"))

    @app.route("/workplaces")
    @admin_required
    def workplaces_list():
        workplaces = Workplace.query.order_by(Workplace.title.asc()).all()
        employees = User.query.order_by(User.full_name.asc()).all()
        return render_template("workplaces_list.html", workplaces=workplaces, employees=employees)

    @app.route("/workplaces/create", methods=["GET", "POST"])
    @admin_required
    def workplace_create():
        employees = User.query.order_by(User.full_name.asc()).all()
        if request.method == "POST":
            workplace = Workplace(
                title=request.form.get("title", "").strip(),
                location=request.form.get("location", "").strip(),
                description=request.form.get("description", "").strip(),
            )
            employee_id = request.form.get("employee_id")
            employee = db.session.get(User, int(employee_id)) if employee_id and employee_id.isdigit() else None
            attach_employee_to_workplace(workplace, employee)
            if not workplace.title or not workplace.location:
                flash("Укажите название и расположение рабочего места.", "danger")
            else:
                db.session.add(workplace)
                db.session.commit()
                flash("Рабочее место создано.", "success")
                return redirect(url_for("workplaces_list"))
        return render_template("workplace_form.html", workplace=None, employees=employees)

    @app.route("/workplaces/<int:workplace_id>/edit", methods=["GET", "POST"])
    @admin_required
    def workplace_edit(workplace_id: int):
        workplace = db.session.get(Workplace, workplace_id) or abort(404)
        employees = User.query.order_by(User.full_name.asc()).all()
        if request.method == "POST":
            workplace.title = request.form.get("title", "").strip()
            workplace.location = request.form.get("location", "").strip()
            workplace.description = request.form.get("description", "").strip()
            employee_id = request.form.get("employee_id")
            employee = db.session.get(User, int(employee_id)) if employee_id and employee_id.isdigit() else None
            attach_employee_to_workplace(workplace, employee)
            if not workplace.title or not workplace.location:
                flash("Укажите название и расположение рабочего места.", "danger")
            else:
                db.session.commit()
                flash("Рабочее место обновлено.", "success")
                return redirect(url_for("workplaces_list"))
        return render_template("workplace_form.html", workplace=workplace, employees=employees)

    @app.route("/workplaces/<int:workplace_id>/delete", methods=["POST"])
    @admin_required
    def workplace_delete(workplace_id: int):
        workplace = db.session.get(Workplace, workplace_id) or abort(404)
        workplace.employee = None
        workplace.equipment_items.update({EquipmentItem.workplace_id: None})
        db.session.delete(workplace)
        db.session.commit()
        flash("Рабочее место удалено.", "info")
        return redirect(url_for("workplaces_list"))

    @app.route("/inventory")
    @login_required
    def inventory_list():
        query = EquipmentItem.query
        if not current_user.is_admin:
            query = query.filter(EquipmentItem.assigned_user_id == current_user.id)
        items = query.order_by(EquipmentItem.category.asc(), EquipmentItem.brand.asc(), EquipmentItem.model.asc()).all()
        return render_template("inventory_list.html", items=items)

    @app.route("/inventory/create", methods=["GET", "POST"])
    @admin_required
    def equipment_create():
        users = User.query.order_by(User.full_name.asc()).all()
        workplaces = Workplace.query.order_by(Workplace.title.asc()).all()
        if request.method == "POST":
            asset_tag = request.form.get("asset_tag", "").strip() or None
            if asset_tag and EquipmentItem.query.filter_by(asset_tag=asset_tag).first():
                flash("Инвентарный номер уже используется.", "danger")
                return render_template("equipment_form.html", item=None, users=users, workplaces=workplaces)
            item = EquipmentItem(
                category=request.form.get("category", "").strip(),
                brand=request.form.get("brand", "").strip(),
                model=request.form.get("model", "").strip(),
                asset_tag=asset_tag,
                serial_number=request.form.get("serial_number", "").strip(),
                status=request.form.get("status", "in_stock"),
                notes=request.form.get("notes", "").strip(),
                purchase_date=parse_date(request.form.get("purchase_date")),
                last_maintenance_date=parse_date(request.form.get("last_maintenance_date")),
            )
            assigned_user_id = request.form.get("assigned_user_id")
            workplace_id = request.form.get("workplace_id")
            if assigned_user_id and assigned_user_id.isdigit():
                item.assigned_user = db.session.get(User, int(assigned_user_id))
            if workplace_id and workplace_id.isdigit():
                item.workplace = db.session.get(Workplace, int(workplace_id))
            if not item.category:
                flash("Укажите категорию техники.", "danger")
            else:
                db.session.add(item)
                db.session.commit()
                flash("Позиция техники добавлена.", "success")
                return redirect(url_for("inventory_list"))
        return render_template("equipment_form.html", item=None, users=users, workplaces=workplaces)

    @app.route("/inventory/<int:item_id>/edit", methods=["GET", "POST"])
    @admin_required
    def equipment_edit(item_id: int):
        item = db.session.get(EquipmentItem, item_id) or abort(404)
        users = User.query.order_by(User.full_name.asc()).all()
        workplaces = Workplace.query.order_by(Workplace.title.asc()).all()
        if request.method == "POST":
            asset_tag = request.form.get("asset_tag", "").strip() or None
            duplicate = EquipmentItem.query.filter(EquipmentItem.asset_tag == asset_tag, EquipmentItem.id != item.id).first()
            if duplicate:
                flash("Инвентарный номер уже используется.", "danger")
                return render_template("equipment_form.html", item=item, users=users, workplaces=workplaces)

            item.category = request.form.get("category", "").strip()
            item.brand = request.form.get("brand", "").strip()
            item.model = request.form.get("model", "").strip()
            item.asset_tag = asset_tag
            item.serial_number = request.form.get("serial_number", "").strip()
            item.status = request.form.get("status", "in_stock")
            item.notes = request.form.get("notes", "").strip()
            item.purchase_date = parse_date(request.form.get("purchase_date"))
            item.last_maintenance_date = parse_date(request.form.get("last_maintenance_date"))
            assigned_user_id = request.form.get("assigned_user_id")
            workplace_id = request.form.get("workplace_id")
            item.assigned_user = db.session.get(User, int(assigned_user_id)) if assigned_user_id and assigned_user_id.isdigit() else None
            item.workplace = db.session.get(Workplace, int(workplace_id)) if workplace_id and workplace_id.isdigit() else None
            if not item.category:
                flash("Укажите категорию техники.", "danger")
            else:
                db.session.commit()
                flash("Позиция техники обновлена.", "success")
                return redirect(url_for("inventory_list"))
        return render_template("equipment_form.html", item=item, users=users, workplaces=workplaces)

    @app.route("/inventory/<int:item_id>/delete", methods=["POST"])
    @admin_required
    def equipment_delete(item_id: int):
        item = db.session.get(EquipmentItem, item_id) or abort(404)
        db.session.delete(item)
        db.session.commit()
        flash("Позиция техники удалена.", "info")
        return redirect(url_for("inventory_list"))

    @app.route("/maintenance", methods=["GET", "POST"])
    @admin_required
    def maintenance():
        items = EquipmentItem.query.order_by(EquipmentItem.category.asc(), EquipmentItem.brand.asc()).all()
        if request.method == "POST":
            item_id = request.form.get("equipment_item_id")
            item = db.session.get(EquipmentItem, int(item_id)) if item_id and item_id.isdigit() else None
            performed_at = parse_date(request.form.get("performed_at"))
            if not item or not performed_at:
                flash("Укажите технику и дату обслуживания.", "danger")
            else:
                log = MaintenanceLog(
                    equipment_item=item,
                    performed_at=performed_at,
                    notes=request.form.get("notes", "").strip(),
                    created_by_id=current_user.id,
                )
                item.last_maintenance_date = performed_at
                db.session.add(log)
                db.session.commit()
                flash("Запись о техосмотре добавлена.", "success")
                return redirect(url_for("maintenance"))
        logs = MaintenanceLog.query.order_by(MaintenanceLog.performed_at.desc(), MaintenanceLog.id.desc()).limit(50).all()
        stale_computers = [item for item in items if item.maintenance_is_stale]
        return render_template("maintenance.html", items=items, logs=logs, stale_computers=stale_computers)

    @app.route("/cartridges")
    @admin_required
    def cartridges_list():
        cartridges = CartridgeItem.query.order_by(CartridgeItem.name.asc()).all()
        return render_template("cartridges_list.html", cartridges=cartridges)

    @app.route("/cartridges/create", methods=["GET", "POST"])
    @admin_required
    def cartridge_create():
        if request.method == "POST":
            cartridge = CartridgeItem(
                name=request.form.get("name", "").strip(),
                vendor=request.form.get("vendor", "").strip(),
                printer_models=request.form.get("printer_models", "").strip(),
                quantity_in_stock=safe_int(request.form.get("quantity_in_stock"), 0),
                quantity_waiting_refill=safe_int(request.form.get("quantity_waiting_refill"), 0),
                notes=request.form.get("notes", "").strip(),
            )
            if not cartridge.name:
                flash("Укажите название картриджа.", "danger")
            else:
                db.session.add(cartridge)
                db.session.commit()
                flash("Картридж добавлен.", "success")
                return redirect(url_for("cartridges_list"))
        return render_template("cartridge_form.html", cartridge=None)

    @app.route("/cartridges/<int:cartridge_id>/edit", methods=["GET", "POST"])
    @admin_required
    def cartridge_edit(cartridge_id: int):
        cartridge = db.session.get(CartridgeItem, cartridge_id) or abort(404)
        if request.method == "POST":
            cartridge.name = request.form.get("name", "").strip()
            cartridge.vendor = request.form.get("vendor", "").strip()
            cartridge.printer_models = request.form.get("printer_models", "").strip()
            cartridge.quantity_in_stock = safe_int(request.form.get("quantity_in_stock"), 0)
            cartridge.quantity_waiting_refill = safe_int(request.form.get("quantity_waiting_refill"), 0)
            cartridge.notes = request.form.get("notes", "").strip()
            if not cartridge.name:
                flash("Укажите название картриджа.", "danger")
            else:
                db.session.commit()
                flash("Картридж обновлён.", "success")
                return redirect(url_for("cartridges_list"))
        return render_template("cartridge_form.html", cartridge=cartridge)

    @app.route("/cartridges/<int:cartridge_id>/delete", methods=["POST"])
    @admin_required
    def cartridge_delete(cartridge_id: int):
        cartridge = db.session.get(CartridgeItem, cartridge_id) or abort(404)
        db.session.delete(cartridge)
        db.session.commit()
        flash("Картридж удалён.", "info")
        return redirect(url_for("cartridges_list"))

    @app.route("/calendar")
    @login_required
    def calendar_view():
        today = datetime.utcnow().date()
        year = safe_int(request.args.get("year"), today.year)
        month = safe_int(request.args.get("month"), today.month)
        calendar_scope = request.args.get("scope", "all")
        if calendar_scope not in {"all", "personal", "shared"}:
            calendar_scope = "all"

        month_calendar = calendar.Calendar(firstweekday=0).monthdatescalendar(year, month)
        visible_start_date = month_calendar[0][0]
        visible_end_date = month_calendar[-1][-1]
        visible_start = datetime.combine(visible_start_date, time.min)
        visible_end = datetime.combine(visible_end_date + timedelta(days=1), time.min)

        query = (
            CalendarEvent.query.join(
                event_participants,
                event_participants.c.event_id == CalendarEvent.id,
                isouter=True,
            )
            .filter(CalendarEvent.starts_at >= visible_start, CalendarEvent.starts_at < visible_end)
        )

        if current_user.is_admin:
            if calendar_scope == "personal":
                query = query.filter(personal_calendar_filter_for(current_user))
            elif calendar_scope == "shared":
                query = query.filter(CalendarEvent.is_shared.is_(True))
        else:
            if calendar_scope == "personal":
                query = query.filter(personal_calendar_filter_for(current_user))
            elif calendar_scope == "shared":
                query = query.filter(CalendarEvent.is_shared.is_(True))
            else:
                query = query.filter(or_(CalendarEvent.is_shared.is_(True), CalendarEvent.created_by_id == current_user.id))

        events = query.distinct().order_by(CalendarEvent.starts_at.asc(), CalendarEvent.title.asc()).all()
        events_by_day: dict = {}
        for event in events:
            events_by_day.setdefault(event.starts_at.date(), []).append(event)

        excursions = (
            ExcursionItem.query.filter(
                ExcursionItem.excursion_date >= visible_start_date,
                ExcursionItem.excursion_date <= visible_end_date,
            )
            .order_by(ExcursionItem.excursion_date.asc(), ExcursionItem.starts_at.asc(), ExcursionItem.id.asc())
            .all()
        )
        excursions_by_day: dict = {}
        for excursion in excursions:
            excursions_by_day.setdefault(excursion.excursion_date, []).append(excursion)

        prev_month = month - 1 or 12
        prev_year = year - 1 if month == 1 else year
        next_month_num = month + 1 if month < 12 else 1
        next_year = year + 1 if month == 12 else year
        users = User.query.order_by(User.department.asc().nullsfirst(), User.full_name.asc()).all()

        return render_template(
            "calendar.html",
            month_calendar=month_calendar,
            events_by_day=events_by_day,
            excursions_by_day=excursions_by_day,
            year=year,
            month=month,
            month_name=calendar.month_name[month],
            prev_month=prev_month,
            prev_year=prev_year,
            next_month_num=next_month_num,
            next_year=next_year,
            calendar_scope=calendar_scope,
            users=users,
        )

    @app.route("/calendar/excursions/import", methods=["POST"])
    @login_required
    def excursion_import():
        if not user_can_import_excursions(current_user):
            abort(403)

        uploaded_file = request.files.get("excursions_pdf")
        if not uploaded_file or not uploaded_file.filename:
            flash("Прикрепите PDF-файл с расписанием экскурсий.", "danger")
            return redirect(url_for("calendar_view", month=request.form.get("month"), year=request.form.get("year"), scope=request.form.get("scope", "all")))

        filename = uploaded_file.filename.lower()
        if not filename.endswith(".pdf"):
            flash("Нужен именно PDF-файл с расписанием экскурсий.", "danger")
            return redirect(url_for("calendar_view", month=request.form.get("month"), year=request.form.get("year"), scope=request.form.get("scope", "all")))

        try:
            excursion_date, excursions = parse_excursions_pdf(uploaded_file)
            imported_count = replace_excursions_for_date(excursion_date, excursions, current_user)
            db.session.commit()
        except Exception as exc:
            db.session.rollback()
            flash(f"Не удалось импортировать экскурсии: {exc}", "danger")
            return redirect(url_for("calendar_view", month=request.form.get("month"), year=request.form.get("year"), scope=request.form.get("scope", "all")))

        flash(
            f"Импорт выполнен: {imported_count} экскурсий на {excursion_date.strftime('%d.%m.%Y')}. Если на эту дату уже были экскурсии, они перезаписаны.",
            "success",
        )
        return redirect(url_for("calendar_view", month=excursion_date.month, year=excursion_date.year, scope=request.form.get("scope", "all")))

    @app.route("/excursion-guide-aliases", methods=["GET", "POST"])
    @admin_required
    def excursion_guide_aliases():
        if request.method == "POST":
            alias_name = normalize_spaces(request.form.get("alias_name"))
            user_id = request.form.get("user_id")
            normalized = normalize_person_name(alias_name)
            selected_user = db.session.get(User, int(user_id)) if user_id and user_id.isdigit() else None

            if not alias_name or not normalized:
                flash("Укажите имя экскурсовода из PDF.", "danger")
            elif not selected_user:
                flash("Выберите сотрудника сайта, которому соответствует это имя.", "danger")
            elif ExcursionGuideAlias.query.filter(ExcursionGuideAlias.alias_normalized == normalized).first():
                flash("Такое соответствие уже существует.", "warning")
            else:
                db.session.add(
                    ExcursionGuideAlias(
                        alias_name=alias_name,
                        alias_normalized=normalized,
                        user=selected_user,
                    )
                )
                db.session.commit()
                flash("Соответствие экскурсовода сохранено.", "success")
                return redirect(url_for("excursion_guide_aliases"))

        aliases = ExcursionGuideAlias.query.order_by(ExcursionGuideAlias.alias_name.asc()).all()
        users = User.query.order_by(User.department.asc().nullsfirst(), User.full_name.asc()).all()
        return render_template("excursion_aliases.html", aliases=aliases, users=users)

    @app.route("/excursion-guide-aliases/<int:alias_id>/delete", methods=["POST"])
    @admin_required
    def excursion_guide_alias_delete(alias_id: int):
        alias = db.session.get(ExcursionGuideAlias, alias_id) or abort(404)
        db.session.delete(alias)
        db.session.commit()
        flash("Соответствие удалено.", "info")
        return redirect(url_for("excursion_guide_aliases"))

    @app.route("/calendar/events/create", methods=["GET", "POST"])
    @login_required
    def event_create():
        users = User.query.order_by(User.department.asc().nullsfirst(), User.full_name.asc()).all()
        departments = get_departments()
        if request.method == "POST":
            starts_at = parse_datetime(request.form.get("starts_at"))
            ends_at = parse_datetime(request.form.get("ends_at"))
            is_shared = bool(request.form.get("is_shared"))
            responsible_id = request.form.get("responsible_user_id")
            responsible_user = (
                db.session.get(User, int(responsible_id)) if is_shared and responsible_id and responsible_id.isdigit() else None
            )
            if not is_shared:
                responsible_user = current_user
            participants = normalize_event_participants(request.form, current_user, responsible_user, is_shared)
            event = CalendarEvent(
                title=request.form.get("title", "").strip(),
                description=request.form.get("description", "").strip(),
                location=request.form.get("location", "").strip(),
                starts_at=starts_at,
                ends_at=ends_at,
                is_shared=is_shared,
                creator=current_user,
                responsible_user=responsible_user or current_user,
                participants=participants,
            )
            if not event.title or not starts_at or not ends_at or ends_at <= starts_at:
                flash("Проверьте название, дату и время мероприятия.", "danger")
            elif is_shared and not responsible_user:
                flash("Для общего мероприятия нужно выбрать ответственного.", "danger")
            else:
                db.session.add(event)
                db.session.flush()
                notify_event_created(event)
                db.session.commit()
                flash("Мероприятие создано.", "success")
                return redirect(url_for("event_detail", event_id=event.id))
        return render_template("event_form.html", event=None, users=users, departments=departments)

    @app.route("/calendar/events/<int:event_id>")
    @login_required
    def event_detail(event_id: int):
        event = db.session.get(CalendarEvent, event_id) or abort(404)
        event_permission_or_404(event)
        return render_template("event_detail.html", event=event)

    @app.route("/calendar/events/<int:event_id>/edit", methods=["GET", "POST"])
    @login_required
    def event_edit(event_id: int):
        event = db.session.get(CalendarEvent, event_id) or abort(404)
        event_permission_or_404(event)
        event_edit_permission_or_404(event)
        users = User.query.order_by(User.department.asc().nullsfirst(), User.full_name.asc()).all()
        departments = get_departments()
        if request.method == "POST":
            starts_at = parse_datetime(request.form.get("starts_at"))
            ends_at = parse_datetime(request.form.get("ends_at"))
            is_shared = bool(request.form.get("is_shared"))
            responsible_id = request.form.get("responsible_user_id")
            responsible_user = (
                db.session.get(User, int(responsible_id)) if is_shared and responsible_id and responsible_id.isdigit() else None
            )
            if not is_shared:
                responsible_user = event.creator

            previous_user_ids = {user.id for user in event.participants}
            previous_user_ids.add(event.responsible_user_id)
            previous_user_ids.add(event.created_by_id)

            event.title = request.form.get("title", "").strip()
            event.description = request.form.get("description", "").strip()
            event.location = request.form.get("location", "").strip()
            event.starts_at = starts_at
            event.ends_at = ends_at
            event.is_shared = is_shared
            event.responsible_user = responsible_user or event.creator
            event.participants = normalize_event_participants(request.form, event.creator, responsible_user, is_shared)

            if not event.title or not starts_at or not ends_at or ends_at <= starts_at:
                flash("Проверьте название, дату и время мероприятия.", "danger")
            elif is_shared and not responsible_user:
                flash("Для общего мероприятия нужно выбрать ответственного.", "danger")
            else:
                affected_user_ids = previous_user_ids | {user.id for user in event.participants} | {event.responsible_user_id}
                notify_event_updated(event, affected_user_ids)
                db.session.commit()
                flash("Мероприятие обновлено.", "success")
                return redirect(url_for("event_detail", event_id=event.id))
        return render_template("event_form.html", event=event, users=users, departments=departments)

    @app.route("/calendar/events/<int:event_id>/delete", methods=["POST"])
    @login_required
    def event_delete(event_id: int):
        event = db.session.get(CalendarEvent, event_id) or abort(404)
        event_permission_or_404(event)
        event_edit_permission_or_404(event)
        affected_user_ids = set()
        if event.is_shared:
            affected_user_ids = {user.id for user in event.participants} | {event.responsible_user_id}
            affected_user_ids.discard(current_user.id)
        title = event.title
        starts_at = event.starts_at
        db.session.delete(event)
        notify_event_deleted(title, starts_at, affected_user_ids)
        db.session.commit()
        flash("Мероприятие удалено.", "info")
        return redirect(url_for("calendar_view"))

    @app.route("/notifications")
    @login_required
    def notifications_list():
        notifications = current_user.notifications.order_by(Notification.created_at.desc()).all()
        return render_template("notifications.html", notifications=notifications)

    @app.route("/notifications/<int:notification_id>/read", methods=["POST"])
    @login_required
    def notification_read(notification_id: int):
        notification = db.session.get(Notification, notification_id) or abort(404)
        if notification.user_id != current_user.id:
            abort(403)
        notification.is_read = True
        db.session.commit()
        flash("Уведомление отмечено как прочитанное.", "success")
        if notification.link:
            return redirect(notification.link)
        return redirect(request.referrer or url_for("notifications_list"))

    @app.route("/notifications/read-all", methods=["POST"])
    @login_required
    def notifications_read_all():
        current_user.notifications.filter_by(is_read=False).update({Notification.is_read: True})
        db.session.commit()
        flash("Все уведомления отмечены как прочитанные.", "success")
        return redirect(url_for("notifications_list"))


app = create_app()


if __name__ == "__main__":
    app.run(debug=True)
