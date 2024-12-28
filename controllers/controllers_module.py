from ascender.common.module import AscModule

from controllers.test_consume.consume_module import ConsumeModule


@AscModule(
    imports=[ConsumeModule],
    declarations=[],
    providers=[],
    exports=[]
)
class ControllersModule:
    ...