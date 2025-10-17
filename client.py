import os

from langchain.chat_models import init_chat_model

import os

# create a key for openIA

# llm_gpt_gpt_4o = init_chat_model("gpt-4o")


def mllOpenIA(model: str, token_counter=None):
    open_ai_key = os.getenv('OPENAI_API_KEY')

    if token_counter is None:
        return init_chat_model(
            model,
        )
    else:
        return init_chat_model(
            model,
            callbacks=[token_counter],
        )
