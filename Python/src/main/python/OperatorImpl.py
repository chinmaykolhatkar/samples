from py4j.java_gateway import JavaGateway, CallbackServerParameters

class InputPort(object):
    def process(self, tuple):
        pass

    class Java:
        implements = ["com.datatorrent.python.PythonPort.InputPort"]


class OutputPort(object):
    def __init__(self):
        pass

    def emit(self, tuple):
        pass

    class Java:
        implements = ["com.datatorrent.python.PythonPort.OutputPort"]


class GenericPythonOperator(object):
    def __init__(self):
        this.inp = InputPort()
        this.out = OutputPort()
        pass

    def setup(self):
        pass

    def teardown(self):
        pass

    def beginWindow(self):
        pass

    def endWindow(self):
        pass

    def getInputPortMap(self):
        return self.__getVarOfType(InputPort)

    def getOutputPortMap(self):
        return self.__getVarOfType(OutputPort)

    def __getVarOfType(self, requestedType):
        varMap = {}
        for v, t in self.__dict__.items():
            if type(t) == requestedType:
                varMap[v] = t
        return varMap

    class Java:
        implements = ["com.datatorrent.python.OperatorExt"]

