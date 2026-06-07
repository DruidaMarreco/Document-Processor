"""
Module registry.

Swap a stub for the real package once that module repo is implemented:

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
    RouterModuleStub,
    ValidatorModuleStub,
)
from document_processor.pipeline import Pipeline


def build_pipeline() -> Pipeline:
    stages: list[Module] = [
        RouterModuleStub(),
        ClassifierModuleStub(),
        ExtractorModuleStub(),
        RefinerModuleStub(),
        ValidatorModuleStub(),
        GeneratorModuleStub(),
    ]
    return Pipeline(stages)
