"""
Module registry.

Status:
  [x] router      — real implementation (router_module)
  [ ] classifier  — stub
  [ ] extractor   — stub
  [ ] refiner     — stub
  [ ] validator   — stub
  [ ] generator   — stub

To replace a stub once a module repo is implemented:

    # Before:
    from document_processor.modules.stubs import ClassifierModuleStub
    stages.append(ClassifierModuleStub())

    # After (install classifier-module package first):
    from classifier_module import ClassifierModule
    stages.append(ClassifierModule())
"""

from document_processor.modules.base import Module
from document_processor.modules.stubs import (
    ClassifierModuleStub,
    ExtractorModuleStub,
    GeneratorModuleStub,
    RefinerModuleStub,
    ValidatorModuleStub,
)
from document_processor.pipeline import Pipeline
from monitoring_module import MonitoringModule
from router_module import RouterModule


def build_pipeline() -> Pipeline:
    stages: list[Module] = [
        RouterModule(),
        ClassifierModuleStub(),
        ExtractorModuleStub(),
        RefinerModuleStub(),
        ValidatorModuleStub(),
        GeneratorModuleStub(),
    ]
    pipeline = Pipeline(stages)
    pipeline.add_listener(MonitoringModule().on_event)
    return pipeline
