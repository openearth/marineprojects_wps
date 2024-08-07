from pywps.app import Process
from pywps.inout.outputs import LiteralOutput
from pywps.app.Common import Metadata

# http://localhost:5000/wps?service=wps&request=GetCapabilities&version=2.0.0
# http://localhost:5000/wps?request=GetCapabilities&service=WPS&version=1.0.0
# http://localhost:5000/wps?request=Execute&service=WPS&version=2.0.0&Identifier=ultimate_question


class UltimateQuestion(Process):
    def __init__(self):
        inputs = []
        outputs = [
            LiteralOutput("answer", "Answer to Ultimate Question", data_type="string")
        ]

        super(UltimateQuestion, self).__init__(
            self._handler,
            identifier="ultimate_question",
            version="1.3.3.7",
            title="Answer to the ultimate question",
            abstract='The process gives the answer to the ultimate question\
             of "What is the meaning of life?',
            profile="",
            metadata=[
                Metadata("Ultimate Question"),
                Metadata("What is the meaning of life"),
            ],
            inputs=inputs,
            outputs=outputs,
            store_supported=False,
            status_supported=False,
        )

    def _handler(self, request, response):
        response.outputs["answer"].data = "42"
        return response
