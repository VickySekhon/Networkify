from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from services.ai.message_generator import generate_outreach_message

router = APIRouter()

class MessageRequest(BaseModel):
    user: dict
    target_person: dict
    target_company: str
    bridge_person: dict | None = None

@router.post("/generate")
async def generate_message(req: MessageRequest):
    try:
        message = generate_outreach_message(
            user=req.user,
            target_person=req.target_person,
            target_company=req.target_company,
            context={"bridge_person": req.bridge_person}
        )
        return {"message": message}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
