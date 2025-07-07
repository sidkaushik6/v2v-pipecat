import asyncio
import os
import argparse
import aiohttp
import websockets
from dotenv import load_dotenv

# Replace AWS imports with:
from pipecat.services.openai.stt import OpenAISTTService
from pipecat.services.openai.tts import OpenAITTSService
# from pipecat.services.google.llm import GoogleAIModelLLMService  # For Gemini
from pipecat.services.openai.llm import OpenAILLMService

from pipecat.processors.filters.frame_filter import FrameFilter
from pipecat.processors.logger import FrameLogger
from pipecat.processors.producer_processor import ProducerProcessor
from pipecat.processors.consumer_processor import ConsumerProcessor
from pipecat.pipeline.parallel_pipeline import ParallelPipeline
from pipecat.processors.frame_processor import FrameProcessor, FrameDirection


from pipecat.serializers.base_serializer import FrameSerializer
from pipecat.frames.frames import AudioRawFrame, Frame, TextFrame  # Added TextFrame


from pipecat.audio.vad.silero import SileroVADAnalyzer, VADParams
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.openai_llm_context import OpenAILLMContext
from pipecat.services.aws.stt import AWSTranscribeSTTService
from pipecat.services.aws.tts import AWSPollyTTSService
from pipecat.services.aws.llm import AWSBedrockLLMService
from pipecat.transports.network.fastapi_websocket import (
    FastAPIWebsocketTransport,
    FastAPIWebsocketParams
)
from pipecat_flows import FlowManager
from flow import flow_config

load_dotenv(override=True)

# Update FrameLogger to be a proper FrameProcessor

class FrameLogger(FrameProcessor):
    def __init__(self, name: str):
        super().__init__()
        self._name = name
        
    async def process_frame(self, frame: Frame, direction: FrameDirection) -> Frame:
        """Official PipeCat processor interface requires both frame and direction"""
        print(f"{self._name} [{direction.name}]: {type(frame).__name__}")
        return frame  # Must return frame to continue pipeline

class VADAwareSerializer(FrameSerializer):
    @property
    def type(self):
        return "binary"

    async def serialize(self, frame: Frame) -> bytes:
        if isinstance(frame, AudioRawFrame):
            return frame.audio
        elif isinstance(frame, TextFrame):
            return frame.text.encode('utf-8')
        return b""

    async def deserialize(self, data: bytes) -> Frame:
        try:
            # Try to decode as text
            text = data.decode('utf-8')
            if text.startswith("VAD_"):
                return TextFrame(text)
        except UnicodeDecodeError:
            pass
        return AudioRawFrame(data)

class LoggingVADAnalyzer(SileroVADAnalyzer):
    async def process_frame(self, frame: Frame) -> Frame:
        if isinstance(frame, AudioRawFrame):
            vad_result = self._analyze_audio(frame.audio)
            if vad_result == "start":
                await self.push_frame(TextFrame("VAD_START"))  # Fixed method
            elif vad_result == "stop":
                await self.push_frame(TextFrame("VAD_STOP"))  # Fixed method
        return await super().process_frame(frame)

async def main(connection_id: int):
    server_host = os.getenv("HOST", "localhost")
    server_port = os.getenv("FAST_API_PORT", "7860")
    ws_url = f"ws://{server_host}:{server_port}/bot_ws/{connection_id}"
    print(f"Bot connecting to server at {ws_url}")

    async with websockets.connect(ws_url) as websocket:
        async with aiohttp.ClientSession() as session:
            vad_analyzer = LoggingVADAnalyzer(params=VADParams(stop_secs=0.5))
        
            transport = FastAPIWebsocketTransport(
                websocket=websocket,
                params=FastAPIWebsocketParams(
                    audio_in_enabled=True,
                    audio_out_enabled=True,
                    vad_analyzer=vad_analyzer,
                    serializer=VADAwareSerializer()  # Updated serializer
                )
            )

            # stt = AWSTranscribeSTTService(
            #     api_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            #     aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            #     aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
            #     region=os.getenv("AWS_REGION")
            # )
            # tts = AWSPollyTTSService(
            #     api_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            #     aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            #     aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
            #     region=os.getenv("AWS_REGION"),
            #     voice_id="Joanna",
            #     params=AWSPollyTTSService.InputParams(
            #         engine="generative",
            #         language="en-AU",
            #         rate="1.1"
            #     )
            # )

            # llm = AWSBedrockLLMService(
            #     aws_access_key=os.getenv("AWS_ACCESS_KEY_ID"),
            #     aws_secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            #     aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
            #     aws_region=os.getenv("AWS_REGION"),
            #     model="anthropic.claude-3-5-sonnet-20240620-v1:0",
            #     params=AWSBedrockLLMService.InputParams(
            #         temperature=0.3,
            #         latency="optimized",
            #         additional_model_request_fields={}
            #     )
            # )
            
            
            # STT Service (OpenAI Whisper)
            stt = OpenAISTTService(
                api_key=os.getenv("OPENAI_API_KEY"),
                model="whisper-large-v3"  # Latest as of 2025
            )

            # TTS Service (OpenAI)
            tts = OpenAITTSService(
                api_key=os.getenv("OPENAI_API_KEY"),
                voice="echo",  # Latest 2025 voices: echo, nova, shimmer
                speed=1.1
            )

            # # LLM Service (Google Gemini)
            # llm = GoogleAIModelLLMService(
            #     api_key=os.getenv("GOOGLE_API_KEY"),
            #     model="gemini-1.5-flash-latest",  # Fastest model as of 2025
            #     temperature=0.3
            # )
            
            llm = OpenAILLMService(
                api_key=os.getenv("OPENAI_API_KEY"),
                model="gpt-4-turbo"
            )

            context = OpenAILLMContext()
            context_aggregator = llm.create_context_aggregator(context)
            vad_logger = FrameLogger("VAD")

            # pipeline = Pipeline([
            #     transport.input(),
            #     stt,
            #     vad_logger,
            #     context_aggregator.user(),
            #     llm,
            #     tts,
            #     transport.output(),
            #     context_aggregator.assistant(),
            # ])
            
            # Updated pipeline
            # pipeline = Pipeline([
            #     transport.input(),
            #     FrameFilter(lambda frame: isinstance(frame, AudioRawFrame)),  # Critical VAD filter
            #     stt,
            #     context_aggregator.user(),
            #     llm,
            #     context_aggregator.assistant(),  # Must be before TTS
            #     tts,
            #     transport.output()
            # ])
            # pipeline = Pipeline([
            #     transport.input(),
            #     FrameFilter(lambda frame: isinstance(frame, AudioRawFrame)),  # VAD filter
            #     stt,
            #     context_aggregator.user(),
            #     llm,
            #     context_aggregator.assistant(),
            #     tts,
            #     transport.output()
            # ])
            
            # Create producer to route VAD events and audio separately
            vad_producer = ProducerProcessor(
                filter=lambda frame: isinstance(frame, (TextFrame, AudioRawFrame)),
                passthrough=False  # Don't send original frames downstream
            )

            # VAD event consumer (handles VAD_START/VAD_STOP)
            vad_consumer = ConsumerProcessor(
                producer=vad_producer,
                transformer=lambda frame: frame if isinstance(frame, TextFrame) else None
            )

            # Audio processing consumer
            audio_consumer = ConsumerProcessor(
                producer=vad_producer,
                transformer=lambda frame: frame if isinstance(frame, AudioRawFrame) else None
            )
            
            # pipeline = Pipeline([
            #     transport.input(),
                
            #     # Separate VAD events from audio stream
            #     Fork(
            #         [
            #             # VAD event branch (TextFrame)
            #             FrameFilter(TextFrame),
            #             # Process VAD events (log or handle)
            #             FrameLogger("VAD")
            #         ],
            #         [
            #             # Audio processing branch
            #             FrameFilter(AudioRawFrame),
            #             stt,
            #             context_aggregator.user(),
            #             llm,
            #             context_aggregator.assistant(),
            #             tts,
            #             transport.output()
            #         ]
            #     )
            # ])
            
            # Build pipeline
            # pipeline = Pipeline([
            #     transport.input(),
            #     vad_producer,
            #     ParallelPipeline(
            #         # Branch 1: VAD event handling
            #         [
            #             vad_consumer,
            #             FrameLogger("VAD")  # Logs VAD events
            #         ],
            #         # Branch 2: Audio processing
            #         [
            #             audio_consumer,
            #             stt,
            #             context_aggregator.user(),
            #             llm,
            #             context_aggregator.assistant(),
            #             tts,
            #             transport.output()
            #         ]
            #     )
            # ])
            
            # Create separate pipelines for VAD and audio
            # vad_pipeline = Pipeline([
            #     FrameFilter(TextFrame),
            #     FrameLogger("VAD")
            # ])

            # audio_pipeline = Pipeline([
            #     FrameFilter(AudioRawFrame),
            #     stt,
            #     context_aggregator.user(),
            #     llm,
            #     context_aggregator.assistant(),
            #     tts,
            #     transport.output()
            # ])

            # # Combine with ParallelPipeline
            # pipeline = Pipeline([
            #     transport.input(),
            #     ParallelPipeline([vad_pipeline, audio_pipeline])
            # ])
            
            
            # VAD branch (handles VAD_START/VAD_STOP)
            vad_pipeline = Pipeline([
                FrameFilter(TextFrame),  # Only TextFrame passes through
                FrameLogger("VAD Events")
            ])

            # Audio processing branch
            audio_pipeline = Pipeline([
                FrameFilter(AudioRawFrame),  # Only AudioRawFrame passes through
                stt,
                context_aggregator.user(),
                llm,
                context_aggregator.assistant(),
                tts,
                transport.output()
            ])

            # Combined pipeline
            pipeline = Pipeline([
                transport.input(),
                ParallelPipeline([vad_pipeline, audio_pipeline])  # Official parallel processing
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