import logging
import os
import requests
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List, Union

import pandas as pd
from tabulate import tabulate
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from helioscta_api_scrapes.utils import (
    file_utils,
)

from helioscta_api_scrapes import (
    settings,
)

"""
"""

class SlackClient:
    """Client for Slack messaging operations."""
    
    def __init__(
        self,
        token: str = settings.SLACK_BOT_TOKEN,
        default_channel_name: str = settings.SLACK_CHANNEL_NAME,
        default_webhook_url: Optional[str] = settings.SLACK_WEBHOOK_URL,
    ):
        self.token = token
        self.default_channel_name = default_channel_name
        self.default_webhook_url = default_webhook_url
    
    def get_client(self) -> WebClient:
        """Get a Slack WebClient instance."""
        return WebClient(token=self.token)
    
    def get_channel_id(self, channel_name: str) -> Optional[str]:
        """Get channel ID from channel name."""
        client = self.get_client()
        channel_name = channel_name.lstrip('#')
        
        try:
            response = client.conversations_list(types="public_channel,private_channel")
            for channel in response['channels']:
                if channel['name'] == channel_name:
                    return channel['id']
            logging.warning(f"Channel '{channel_name}' not found")
            return None
        
        except SlackApiError as e:
            logging.error(f"Error finding channel: {e.response['error']}")
            return None

    def send_message(
        self,
        message: str,
        channel_name: Optional[str] = None,
        blocks: Optional[List[Dict]] = None,
        thread_ts: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send a message to a Slack channel."""
        client = self.get_client()
        channel_name = channel_name or self.default_channel_name
        
        try:
            response = client.chat_postMessage(
                channel=channel_name,
                text=message,
                blocks=blocks,
                thread_ts=thread_ts,
            )
            return response
        except SlackApiError as e:
            logging.error(f"Error posting message: {e.response['error']}")
            raise

    def send_webhook_message(
        self,
        text: str,
        blocks: Optional[List[Dict]] = None,
        webhook_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send a message via webhook."""
        url = webhook_url or self.default_webhook_url
        
        if not url:
            raise ValueError("webhook_url must be provided either in config or as argument")
        
        payload = {"text": text}
        if blocks:
            payload["blocks"] = blocks
        
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            logging.info("Webhook message sent successfully")
            return {"ok": True, "response": response.text}
        except requests.exceptions.RequestException as e:
            logging.error(f"Error posting webhook message: {str(e)}")
            raise

    def send_success_message(
        self,
        job_name: str,
        run_id: Optional[str] = None,
        message: Optional[str] = None,
        channel_name: Optional[str] = None,
        include_metadata: bool = True,
    ) -> Dict[str, Any]:
        """Send a success notification."""
        default_message = f"✅ *Success*: `{job_name}` completed successfully"
        final_message = message or default_message
        
        if include_metadata and run_id:
            metadata_lines = [
                final_message,
                f"• Run ID: `{run_id}`",
            ]
            final_message = "\n".join(metadata_lines)
        
        return self.send_message(message=final_message, channel_name=channel_name)
    
    def send_failure_message(
        self,
        job_name: str,
        run_id: Optional[str] = None,
        error: Optional[Exception] = None,
        message: Optional[str] = None,
        channel_name: Optional[str] = None,
        include_metadata: bool = True,
    ) -> Dict[str, Any]:
        """Send a failure notification."""
        default_message = f"❌ *Failure*: `{job_name}` failed"
        final_message = message or default_message
        
        message_parts = [final_message]
        
        if error:
            message_parts.append(f"• Error: `{str(error)}`")
        
        if include_metadata and run_id:
            message_parts.append(f"• Run ID: `{run_id}`")
        
        final_message = "\n".join(message_parts)
        return self.send_message(message=final_message, channel_name=channel_name)
    
    def send_warning_message(
        self,
        job_name: str,
        message: str,
        channel_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send a warning notification."""
        warning_message = f"⚠️ *Warning*: `{job_name}`\n{message}"
        return self.send_message(message=warning_message, channel_name=channel_name)
    
    def send_metric_alert(
        self,
        job_name: str,
        metric_name: str,
        current_value: float,
        threshold: float,
        comparison: str = "greater than",
        channel_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send a metric alert notification."""
        message = (
            f"🚨 *Metric Alert*: `{job_name}`\n"
            f"• Metric: {metric_name}\n"
            f"• Current Value: {current_value:,.2f}\n"
            f"• Threshold: {threshold:,.2f} ({comparison})"
        )
        
        return self.send_message(message=message, channel_name=channel_name)

    def send_dataframe(
        self,
        df: pd.DataFrame,
        tablefmt: str = 'simple',
        channel_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send a DataFrame as a formatted table."""
        table = tabulate(df, headers='keys', tablefmt=tablefmt, showindex=False)        
        message = f"```\n{table}\n```"
        return self.send_message(message=message, channel_name=channel_name)

    def send_excel_file(
        self,
        df: pd.DataFrame,
        channel_name: Optional[str] = None,
        filename: str = "data.xlsx",
        sheet_name: str = "Sheet1",
        title: Optional[str] = None,
        initial_comment: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send a DataFrame as an Excel file to Slack."""
        import tempfile
        
        client = self.get_client()
        channel_name = channel_name or self.default_channel_name
        channel_id = self.get_channel_id(channel_name=channel_name)
        
        if not filename.endswith('.xlsx'):
            filename = f"{filename}.xlsx"
        
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.xlsx', delete=False) as tmp_file:
                tmp_path = tmp_file.name
                
                with pd.ExcelWriter(tmp_path, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            with open(tmp_path, 'rb') as file_content:
                response = client.files_upload_v2(
                    channel=channel_id,
                    file=file_content,
                    filename=filename,
                    title=title or filename,
                    initial_comment=initial_comment,
                )
            
            logging.info(f"Excel file '{filename}' uploaded to {channel_id}")
            return response
            
        except SlackApiError as e:
            logging.error(f"Error uploading Excel file: {e.response['error']}")
            raise
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def send_file(
        self,
        file_path: str,
        channel_name: Optional[str] = None,
        filename: Optional[str] = None,
        title: Optional[str] = None,
        initial_comment: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send a file to Slack."""
        client = self.get_client()
        channel_name = channel_name or self.default_channel_name
        channel_id = self.get_channel_id(channel_name=channel_name)
        
        filename = filename or os.path.basename(file_path)
        
        try:
            with open(file_path, 'rb') as file_content:
                response = client.files_upload_v2(
                    channel=channel_id,
                    file=file_content,
                    filename=filename,
                    title=title or filename,
                    initial_comment=initial_comment,
                )
            
            logging.info(f"File '{filename}' uploaded to {channel_id}")
            return response
            
        except SlackApiError as e:
            logging.error(f"Error uploading file: {e.response['error']}")
            raise


def send_pipeline_failure_with_log(
    job_name: str,
    error: Exception,
    log_file_path: Optional[Union[str, Path]] = None,
    channel_name: str = "#test123",
) -> Dict[str, Any]:
    """
    Send a pipeline failure notification with log file attachment.
    """
    
    # init client
    client = SlackClient(default_channel_name=channel_name)

    # create message
    mst_timestamp = file_utils.get_mst_timestamp()
    current_datetime = mst_timestamp.strftime('%a %b-%d %H:%M')
        
    message = (
        f"❌ *Pipeline Failed: `{job_name}`*\n\n"
        f"*Error:* `{type(error).__name__}: {str(error)}`\n"
        f"*Time:* {current_datetime}\n"
    )
    
    if log_file_path:
        log_path = Path(log_file_path)
        if log_path.exists():
            try:
                client.send_file(
                    file_path=str(log_path),
                    channel_name=channel_name,
                    initial_comment=message,
                )
            except Exception as e:
                logging.error(f"Failed to upload log file: {e}")
    else:
        client.send_message(
            channel_name=channel_name,
            message=message, 
        )