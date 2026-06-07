"""
Module registry — all stubs replaced with real implementations.

Status:
  [x] router      — router_module.RouterModule
  [x] classifier  — classifier_module.ClassifierModule
  [x] extractor   — extractor_module.ExtractorModule
  [x] refiner     — refiner_module.RefinerModule
  [x] validator   — validator_module.ValidatorModule
  [x] generator   — generator_module.GeneratorModule
  [x] monitoring  — monitoring_module.MonitoringModule (listener)

To add a new module, import it here, append to `stages`, and remove its stub.
"""

from classifier_module import ClassifierModule
from extractor_module import ExtractorModule
from generator_module import GeneratorModule
from monitoring_module import MonitoringModule
from refiner_module import RefinerModule
from router_module import RouterModule
from validator_module import ValidatorModule

from document_processor.modules.base import Module
from document_processor.pipeline import Pipeline


def build_pipeline() -> Pipeline:
    stages: list[Module] = [
        RouterModule(),
        ClassifierModule(),
        ExtractorModule(),
        RefinerModule(),
        ValidatorModule(),
        GeneratorModule(),
    ]
    pipeline = Pipeline(stages)
    pipeline.add_listener(MonitoringModule().on_event)
    return pipeline
