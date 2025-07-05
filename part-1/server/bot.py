# bot.py

import asyncio
import os
import argparse
import aiohttp
import websockets
from dotenv import load_dotenv

from pipecat.audio.vad.silero import SileroVADAnalyzer, VADParams
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.openai_llm_context import OpenAILLMContext
from pipecat.services.aws.stt import AWSTranscribeSTTService
from pipecat.services.aws.tts import AWSPollyTTSService
from pipecat.services.aws.llm import AWSBedrockLLMService

# ✨ Updated imports for FastAPI WebSocket transport
from pipecat.transports.network.fastapi_websocket import (
    FastAPIWebsocketTransport,
    FastAPIWebsocketParams
)

from pipecat.frames.frames import AudioRawFrame, Frame

from pipecat.serializers.twilio import TwilioFrameSerializer

from pipecat_flows import FlowManager
from flow import flow_config

load_dotenv(override=True)


# === VAD LOGGER EXTENSION ===
class LoggingVADAnalyzer(SileroVADAnalyzer):
    async def process_frame(self, frame: Frame) -> Frame:
        if isinstance(frame, AudioRawFrame):
            vad_result = self._analyze_audio(frame.audio)
            if vad_result == "start":
                print("VAD: Speech detected")
            elif vad_result == "stop":
                print("VAD: Speech ended")
        return await super().process_frame(frame)
# ==================================


async def main(connection_id: int):
    server_host = os.getenv("HOST", "localhost")
    server_port = os.getenv("FAST_API_PORT", "7860")
    ws_url = f"ws://{server_host}:{server_port}/bot_ws/{connection_id}"
    print(f"Bot connecting to server at {ws_url}")

    async with websockets.connect(ws_url) as websocket:
        async with aiohttp.ClientSession() as session:
            transport = FastAPIWebsocketTransport(
                websocket=websocket,
                params=FastAPIWebsocketParams(
                    audio_in_enabled=True,
                    audio_out_enabled=True,
                    vad_enabled=True,
                    vad_analyzer=LoggingVADAnalyzer(params=VADParams(stop_secs=0.5)),
                    serializer=TwilioFrameSerializer(stream_sid="dummy")
                )
            )

            # AWS service setups (unchanged)
            stt = AWSTranscribeSTTService(
                api_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
                region=os.getenv("AWS_REGION")
            )
            tts = AWSPollyTTSService(
                api_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
                region=os.getenv("AWS_REGION"),
                voice_id="Joanna",
                params=AWSPollyTTSService.InputParams(
                    engine="generative",
                    language="en-AU",
                    rate="1.1"
                )
            )
            llm = AWSBedrockLLMService(
                aws_access_key=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
                aws_region=os.getenv("AWS_REGION"),
                model="us.anthropic.claude-3-5-haiku-20241022-v1:0",
                params=AWSBedrockLLMService.InputParams(
                    temperature=0.3,
                    latency="optimized",
                    additional_model_request_fields={}
                )
            )

            context = OpenAILLMContext()
            context_aggregator = llm.create_context_aggregator(context)

            pipeline = Pipeline([
                transport.input(),
                stt,
                context_aggregator.user(),
                llm,
                tts,
                transport.output(),
                context_aggregator.assistant(),
            ])

            task = PipelineTask(
                pipeline,
                params=PipelineParams(
                    allow_interruptions=True,
                    enable_metrics=True,
                    enable_usage_metrics=True,
                ),
            )

            flow_manager = FlowManager(
                task=task,
                llm=llm,
                context_aggregator=context_aggregator,
                tts=tts,
                flow_config=flow_config,
            )

            await flow_manager.initialize()

            runner = PipelineRunner(handle_sigint=False)
            await runner.run(task)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Voice Bot")
    parser.add_argument("--connection_id", type=int, required=True, help="Connection ID")
    config = parser.parse_args()
    asyncio.run(main(config.connection_id))
