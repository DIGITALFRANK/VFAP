from modules.constants import constant
class IOError(Exception):
    def __init__(self, moduleName, exeptionType, message):
        if moduleName is None and exeptionType is None and message is None:
            self.moduleName = constant.MODULE_UNIDENTIFIED
            self.exeptionType=''
            self.message=''
        else:
            self.message = message
            self.moduleName=moduleName
            self.exeptionType=exeptionType

    def __str__(self):
        return str("Exception occurred in: " + self.moduleName + ", Exception type: " + self.exeptionType + ", Message: " + self.message)