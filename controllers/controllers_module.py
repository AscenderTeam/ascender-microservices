from ascender.common.module import AscModule

from controllers.test_consume.consume_module import ConsumeModule
from controllers.test_global.consume_module import ConsumeGlobalModule


@AscModule(
    imports=[ConsumeModule, ConsumeGlobalModule],
    declarations=[],
    providers=[],
    exports=[]
)
class ControllersModule:
    ...