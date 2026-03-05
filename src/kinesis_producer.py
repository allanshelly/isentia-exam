"""AWS Kinesis producer for streaming data"""
import logging
import json
from typing import Dict, List
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class KinesisProducer:
    """Produce records to AWS Kinesis stream"""
    
    def __init__(
        self,
        stream_name: str,
        region_name: str = "us-east-1",
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None
    ):
        """
        Initialize Kinesis producer
        
        Args:
            stream_name: Name of the Kinesis stream
            region_name: AWS region
            aws_access_key_id: AWS access key (uses default credentials if None)
            aws_secret_access_key: AWS secret key (uses default credentials if None)
        """
        self.stream_name = stream_name
        self.region_name = region_name
        
        # Initialize Kinesis client
        if aws_access_key_id and aws_secret_access_key:
            self.client = boto3.client(
                'kinesis',
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )
        else:
            self.client = boto3.client('kinesis', region_name=region_name)
        
        logger.info(f"Initialized Kinesis producer for stream: {stream_name}")
    
    def put_record(self, data: Dict, partition_key: str) -> bool:
        """
        Put a single record into Kinesis stream
        
        Args:
            data: Dictionary to serialize and send
            partition_key: Partition key for the record
            
        Returns:
            True if successful, False otherwise
        """
        try:
            response = self.client.put_record(
                StreamName=self.stream_name,
                Data=json.dumps(data),
                PartitionKey=partition_key
            )
            
            logger.debug(f"Record sent to Kinesis. Sequence: {response.get('SequenceNumber')}")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ResourceNotFoundException':
                logger.error(f"Stream not found: {self.stream_name}")
            elif error_code == 'ProvisionedThroughputExceededException':
                logger.error("Provisioned throughput exceeded")
            else:
                logger.error(f"Client error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error putting record to Kinesis: {str(e)}", exc_info=True)
            return False
    
    def put_batch(self, records: List[Dict]) -> int:
        """
        Put multiple records into Kinesis stream using batch operation
        
        Args:
            records: List of dictionaries to send
            
        Returns:
            Number of successfully sent records
        """
        if not records:
            logger.warning("No records to put")
            return 0
        
        successful = 0
        
        for i, record in enumerate(records):
            # Use article_id as partition key to group related records
            partition_key = record.get('article_id', str(i))
            
            if self.put_record(record, partition_key):
                successful += 1
            else:
                logger.warning(f"Failed to send record {i}: {record.get('article_id')}")
        
        logger.info(f"Sent {successful} out of {len(records)} records to Kinesis")
        return successful
    
    def stream_exists(self) -> bool:
        """
        Check if the Kinesis stream exists
        
        Returns:
            True if stream exists, False otherwise
        """
        try:
            self.client.describe_stream(StreamName=self.stream_name)
            logger.info(f"Stream exists: {self.stream_name}")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.error(f"Stream does not exist: {self.stream_name}")
                return False
            else:
                logger.error(f"Error checking stream: {str(e)}")
                return False
        except Exception as e:
            logger.error(f"Unexpected error checking stream: {str(e)}", exc_info=True)
            return False
