import typing as tp

from fastapi import APIRouter, Depends
from loguru import logger
from starlette import status

from dependencies import get_employee_by_card_id
from feecc_workbench import models as mdl
from feecc_workbench.Employee import Employee
from feecc_workbench.WorkBench import WorkBench
from feecc_workbench.exceptions import StateForbiddenError

WORKBENCH = WorkBench()

router = APIRouter(
    prefix="/employee",
    tags=["employee"],
)


@router.post("/info", response_model=mdl.EmployeeOut)
def get_employee_data(employee: mdl.EmployeeWCardModel = Depends(get_employee_by_card_id)) -> mdl.EmployeeOut:
    """return data for an Employee with matching ID card"""
    return mdl.EmployeeOut(
        status_code=status.HTTP_200_OK, detail="Employee retrieved successfully", employee_data=employee
    )


@router.post("/log-in", response_model=tp.Union[mdl.EmployeeOut, mdl.GenericResponse])  # type: ignore
def log_in_employee(
    employee: mdl.EmployeeWCardModel = Depends(get_employee_by_card_id),
) -> tp.Union[mdl.EmployeeOut, mdl.GenericResponse]:
    """handle logging in the Employee at a given Workbench"""
    try:
        WORKBENCH.log_in(Employee(rfid_card_id=employee.rfid_card_id, name=employee.name, position=employee.position))
        return mdl.EmployeeOut(
            status_code=status.HTTP_200_OK, detail="Employee logged in successfully", employee_data=employee
        )

    except StateForbiddenError as e:
        return mdl.GenericResponse(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))


@router.post("/log-out", response_model=mdl.GenericResponse)
def log_out_employee() -> mdl.GenericResponse:
    """handle logging out the Employee at a given Workbench"""
    try:
        WORKBENCH.log_out()
        if WORKBENCH.employee is not None:
            raise ValueError("Unable to logout employee")
        return mdl.GenericResponse(status_code=status.HTTP_200_OK, detail="Employee logged out successfully")

    except Exception as e:
        message: str = f"An error occurred while logging out the Employee: {e}"
        logger.error(message)
        return mdl.GenericResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message)
