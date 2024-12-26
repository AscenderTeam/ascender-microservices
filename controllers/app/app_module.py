from controllers.app.app_service import AppService
from controllers.app.app_controller import AppController
from ascender.common.module import AscModule


@AscModule(
    imports=[
    ],
    declarations=[
        AppController,
    ],
    providers=[
        AppService,
    ],
    exports=[]
)
class AppModule:
    ...