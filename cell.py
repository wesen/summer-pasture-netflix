# this file is a sketch of the objects and structures that will be used in dashboard
# to construct a running cell
import copy
import enum
import logging
from typing import MutableMapping, Optional, Callable, Any

from dataclasses import dataclass

import sqlalchemy.ext.declarative
from sqlalchemy import Column, String, Integer, ForeignKey, Float, create_engine, types
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()


class PrinterStatus(enum.Enum):
    IDLE = 1
    PRINTING = 2
    PRINT_SUCCEEDED = 3
    PRINT_FAILED = 4


class Printer(Base):
    __tablename__ = "printers"

    id = Column(String, primary_key=True, nullable=False)
    status = Column(types.Enum(PrinterStatus))
    current_resin = Column(String)
    print_request = relationship("PrintRequest")

    prints = relationship("Print", back_populates='printer')

    def __str__(self):
        return f"<Printer {self.id} ({self.status}, {self.current_resin})>"

    def tick_time_s(self, time_s):
        pass


class Print(Base):
    __tablename__ = "prints"

    id = Column(Integer, primary_key=True, autoincrement=True)
    printer_id = Column(Integer, ForeignKey("printers.id"))
    printer = relationship("Printer")
    print_request_id = Column(Integer, ForeignKey("print_requests.id"))
    print_request = relationship("PrintRequest", back_populates="print")
    time_elapsed_s = Column(Float)

    def __str__(self):
        return f"<Print #{self.id} printer {self.printer_id} printRequestId #{self.print_request_id}>"


class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    resin_code = Column(String)
    duration_s = Column(Float)

    def __str__(self):
        return f"<Job #{self.id} {self.name} ({self.resin_code})>"


class PrintQueueEntry(Base):
    __tablename__ = "print_queue_entries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey("jobs.id"))
    job = relationship("Job", backref="print_queue_entries")
    position = Column(Integer)
    resin_code = Column(String)
    print_request = relationship("PrintRequest")

    def __str__(self):
        return f"<PrintQueueEntry #{self.id} jobId #{self.job_id} ({self.resin_code}) pos {self.position}>"


class PrintRequest(Base):
    __tablename__ = "print_requests"

    id = Column(Integer, primary_key=True, autoincrement=True)
    print_queue_entry_id = Column(Integer, ForeignKey("print_queue_entries.id"))
    print_queue_entry = relationship("PrintQueueEntry", back_populates="print_request")
    printer_id = Column(Integer, ForeignKey("printers.id"))
    printer = relationship("Printer", back_populates="print_request")
    print = relationship("Print")

    def __str__(self):
        return f"<PrintRequest #{self.id} printer {self.printer_id} printQueueEntryId #{self.print_queue_entry_id}>"


class ChangeEvent(enum.Enum):
    ADDED_OBJECT = 1
    REMOVED_OBJECT = 2
    UPDATED_OBJECT = 3


engine = create_engine("sqlite:///:memory:")
Base.metadata.create_all(engine)
Session = sessionmaker(engine, expire_on_commit=False)


def run_in_session(f: Callable[[Any], None], session: object = None) -> Any:
    if session is not None:
        return f(session)
    else:
        with Session.begin() as session:
            return f(session)


class Blackboard:
    """
    Stores persistent state (using SQLAlchemy).

    Objects can be added, removed or updated.
    Each action creates a relevant notification, which is stored for further processing.

    Callbacks for each notification namespace can be registered using `subscribe_ns`
    """
    def __init__(self):
        self.callbacks = {}
        self.global_time_s = 0
        self.notifications = []

    def add_object(self, o, session=None):
        """
        Add an object to the session, and notify the object class' namespace.
        """
        class_name = o.__class__.__name__

        def f(session_):
            session_.add(o)
            session_.flush()
            if class_name not in self.callbacks:
                self.callbacks[class_name] = []

        run_in_session(f, session=session)
        self.notify_ns(class_name, ChangeEvent.ADDED_OBJECT, o)

    def remove_object(self, o, session=None):
        """
        Remove an object from the session, and notify the object class' namespace.
        """
        class_name = o.__class__.__name__

        def f(session_):
            session_.delete(o)

        run_in_session(f, session=session)
        self.notify_ns(class_name, ChangeEvent.REMOVED_OBJECT, o)

    def update_object(self, o, session=None):
        """
        Update an object in the session, and notify the object class' namespace.
        """
        class_name = o.__class__.__name__

        def f(session_):
            session_.add(o)
            session_.flush()

        run_in_session(f, session)
        self.notify_ns(class_name, ChangeEvent.UPDATED_OBJECT, o)

    def get_objects(self, ns_name: str, session=None):
        class_ = globals()[ns_name]

        def f(session_):
            return session_.query(class_).all()

        return run_in_session(f, session)

    def notify_ns(self, ns_name: str, event: ChangeEvent, data):
        self.notifications += [(ns_name, event, copy.deepcopy(data))]

    def flush_notifications(self):
        """
        Execute all stored notifications.
        """
        _notifications = self.notifications
        print(f"flushing notifications: {len(_notifications)} new notifications")
        self.notifications = []
        for ns_name, event, data in _notifications:
            print(f" - {ns_name} {event} {data}")
            if ns_name in self.callbacks:
                for f in self.callbacks[ns_name]:
                    f(ns_name, event, data)
        print(f"flushed notifications")

    def subscribe_ns(self, ns_name, f):
        """
        Register a callback called for events in namespace `ns_name`
        """
        if ns_name not in self.callbacks:
            self.callbacks[ns_name] = []

        self.callbacks[ns_name] += [f]

    def print(self):
        with Session.begin() as session:
            for class_ in [Job, Printer, PrintQueueEntry, PrintRequest, Print]:
                print(f"{class_.__name__}:")
                for o in session.query(class_).all():
                    print(f"- {o}")

    def tick_time_s(self, duration_s):
        self.global_time_s += duration_s

    # list of actions
    def user_adds_job(self, job: Job):
        print(f"EVT: User adds job {job}")
        self.add_object(job)

    def user_requests_print(self, job_id: int, n: int):
        print(f"EVT: User requests job {job_id} printed {n} times")
        with Session.begin() as session:
            try:
                job = session.query(Job).filter_by(id=job_id).first()
                if job is None:
                    raise RuntimeError(f"No such job {job_id}")

                last_pos = 0
                # XXX
                # last_pos = session.query(PrintQueueEntry.position).max()
                for print_queue_entry in session.query(PrintQueueEntry).all():
                    if print_queue_entry.position > last_pos:
                        last_pos = print_queue_entry.position

                for i in range(n):
                    pqe = PrintQueueEntry(job_id=job_id, position=last_pos + i, resin_code=job.resin_code)
                    self.add_object(pqe, session=session)
            except:
                logging.error("Could not request job", exc_info=True)

    def printer_heartbeat(self, serial, status, current_resin):
        print(f"EVT: {serial} heartbeats")
        with Session.begin() as session:
            printer = session.query(Printer).filter_by(id=serial).first()
            if printer is None:
                self.add_object(Printer(id=serial, status=status, current_resin=current_resin), session=session)
            else:
                changed = False
                if printer.status != status:
                    printer.status = status
                    changed = True
                if printer.current_resin != current_resin:
                    printer.current_resin = current_resin
                    changed = True
                if changed:
                    self.update_object(printer, session=session)


class PrinterScheduler:
    """
    This scheduler listens to:
    * changes in the Printer namespace
    * changes in the PrintQueueEntry namespace

    The basic algorithm is: when a Printer is IDLE, and a PrintQueueEntry matching the printer's resin code
    has no PrintRequest, a new PrintRequest is created and assigned to the PrintQueueEntry.
    """
    def __init__(self, blackboard: Blackboard):
        self.blackboard = blackboard
        self.blackboard.subscribe_ns("Printer",
                                     lambda ns_name, evt, data: self.on_printer_callback(ns_name, evt, data))
        self.blackboard.subscribe_ns("PrintQueueEntry",
                                     lambda ns_name, evt, data: self.on_pqe_callback(ns_name, evt, data))

        with Session.begin() as session:
            for pqe in session.query(PrintQueueEntry).all():
                self.handle_print_queue_entry(pqe)

    def assign_pqe_to_printer(self, printer, session):
        if printer.status == PrinterStatus.IDLE:
            pqe = session.query(PrintQueueEntry) \
                .filter(PrintQueueEntry.print_request == None,
                        PrintQueueEntry.resin_code == printer.current_resin) \
                .order_by(PrintQueueEntry.position.asc()) \
                .first()
            if pqe is not None:
                print(f"Assigning {pqe} to {printer}")
                pr = PrintRequest(print_queue_entry_id=pqe.id, printer_id=printer.id)
                self.blackboard.add_object(pr)

    def assign_printer_to_pqe(self, pqe: PrintQueueEntry, session):
        printer = session.query(Printer) \
            .filter(Printer.current_resin == pqe.resin_code,
                    Printer.status == PrinterStatus.IDLE,
                    Printer.print_request == None).first()
        if printer is not None:
            print(f"Assigning {pqe} to {printer}")
            pr = PrintRequest(print_queue_entry_id=pqe.id, printer_id=printer.id)
            self.blackboard.add_object(pr)

    def on_printer_callback(self, ns_name, evt, printer):
        print(f"CB: {ns_name} {evt} {printer}")
        with Session.begin() as session:
            if evt == ChangeEvent.REMOVED_OBJECT:
                self.handle_printer_removal(printer, session)
            elif evt == ChangeEvent.UPDATED_OBJECT:
                self.handle_printer_update(printer, session)
            elif evt == ChangeEvent.ADDED_OBJECT:
                self.handle_printer_add(printer, session)

    def handle_printer_removal(self, printer, session):
        pr = session.query(PrintRequest).filter_by(printer_id=printer.id).first()
        if pr is not None:
            print(f"Removing print requests {pr}")
            pqe = pr.print_queue_entry
            # XXX if the print is ongoing, what should we do?
            self.blackboard.remove_object(pr, session)
            # we should try to reschedule the print queue entry related to that job
            self.assign_printer_to_pqe(pqe, session)

    def handle_printer_update(self, printer: Printer, session):
        # XXX what if the printer comes back with a totally different print

        # remove a PR if already present but not matching the new resin
        pr = session.query(PrintRequest).filter_by(printer_id=printer.id).first()  # type: Optional[PrintRequest]

        if pr is not None:
            pqe = pr.print_queue_entry  # type: PrintQueueEntry
            # this logic is a bit crude, what about other status changes, etc...
            # XXX the printer probably needs to provide the current print request id it's servicing
            # XXX check if a print is currently running
            if pqe.resin_code != printer.current_resin:
                # maybe only do this if the print request is as of yet idle
                print(f"Removing print request {pr} because printer now has {printer.current_resin})")
                self.blackboard.remove_object(pr, session)
                pr = None

        if pr is None:
            self.assign_pqe_to_printer(printer, session)

    def handle_printer_add(self, printer, session):
        pr = session.query(PrintRequest).filter_by(printer_id=printer.id).first()  # type: Optional[PrintRequest]
        if pr is None:
            self.assign_pqe_to_printer(printer, session)

    def on_pqe_callback(self, ns_name, evt, pqe: PrintQueueEntry):
        print(f"CB: {ns_name} {evt} {pqe}")
        with Session.begin() as session:
            if evt == ChangeEvent.REMOVED_OBJECT:
                self.handle_pqe_removal(pqe, session)
            elif evt == ChangeEvent.UPDATED_OBJECT:
                self.handle_pqe_update(pqe, session)
            elif evt == ChangeEvent.ADDED_OBJECT:
                self.handle_pqe_add(pqe, session)

    def handle_pqe_removal(self, pqe: PrintQueueEntry, session):
        if pqe.print_request is not None:
            print(f"Removing print request {pqe.print_request} for print queue entry {pqe}")
            # XXX we should check if the print_request has a print
            # before removing it, and maybe we should cancel the print here?
            self.blackboard.remove_object(pqe.print_request, session)

    def handle_pqe_update(self, pqe: PrintQueueEntry, session):
        # this means the position has changed

        # if this pqe had a print request before change, discard the print request, and recompute a new one
        if pqe.print_request is not None:
            print(f"Removing print request {pqe.print_request} for print queue entry {pqe}")
            # XXX we should check if the print_request has a print
            # before removing it, and maybe we should cancel the print here?
            self.blackboard.remove_object(pqe.print_request, session)
        self.assign_printer_to_pqe(pqe, session)

    def handle_pqe_add(self, pqe: PrintQueueEntry, session):
        self.assign_printer_to_pqe(pqe, session)


def main():
    bb = Blackboard()

    job_teeth = Job(name="teeth", resin_code="DentalResin", duration_s=3600)
    bb.user_adds_job(job_teeth)
    job_mouthPiece = Job(name="mouthpiece", resin_code="ClearResin", duration_s=3600)
    bb.user_adds_job(job_mouthPiece)

    with Session.begin() as session:
        dental1 = Printer(id="dental1", status=PrinterStatus.IDLE, current_resin="DentalResin")
        dental2 = Printer(id="dental2", status=PrinterStatus.IDLE, current_resin="DentalResin")
        clear1 = Printer(id="clear1", status=PrinterStatus.IDLE, current_resin="ClearResin")
        clear2 = Printer(id="clear2", status=PrinterStatus.IDLE, current_resin="ClearResin")

    printers = [dental1, dental2, clear1, clear2]

    def tick_time_s(time_s=1):
        print("")
        print(f"Tick {time_s}")
        for printer in printers:
            printer.tick_time_s(time_s)

        for printer in printers:
            bb.printer_heartbeat(serial=printer.id, status=printer.status, current_resin=printer.current_resin)

        bb.tick_time_s(time_s)
        bb.flush_notifications()

        print("----")
        bb.print()

    tick_time_s()

    pqs = PrinterScheduler(bb)
    tick_time_s()

    tick_time_s()

    print("")
    bb.user_requests_print(job_teeth.id, 4)
    tick_time_s()

    tick_time_s()

    bb.user_requests_print(job_mouthPiece.id, 3)
    tick_time_s()

    tick_time_s()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
