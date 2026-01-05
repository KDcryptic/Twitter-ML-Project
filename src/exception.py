import sys
import os
import logging

def error_message_details(error,error_detail:sys):
    _,_,exc_tb=error_detail.exc_tb()
    fileName=exc_tb.tb_frame.f_code.co_filename
    error_message = f'An error has occured in file {fileName} at line number {exc_tb.tb_lineno}, {str(error)}'

    return error_message

class CustomException(Exception):
    
    def __init__(self,error_message,error_detail:sys):
        super().__init(error_message)
        self.error_message=error_message_details(error_message,error_detail=error_detail)

    def __str__(self):
        return self.error_message