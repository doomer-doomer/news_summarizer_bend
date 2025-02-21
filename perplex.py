import requests
from bs4 import BeautifulSoup
#from transformers import pipeline
import re
import json
from typing import List, Dict, Any, Optional, Callable, Union, Awaitable, AsyncGenerator
import google.generativeai as genai
from googleapiclient.discovery import build
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
import time
import sys
import hashlib
import logging
from datetime import datetime
import socketio
from aiohttp import web
import aiohttp
from cachetools import TTLCache, LRUCache
from aiohttp import ClientTimeout, TCPConnector
from ratelimit import limits, sleep_and_retry
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import PIL.Image
import io
import numpy as np
from functools import partial
import psutil
import gc
import diskcache
import zlib
import time
from dataclasses import dataclass
from typing import Optional, Dict, List, Any
import asyncio
from concurrent.futures import ProcessPoolExecutor
import PIL.Image
from io import BytesIO
import numpy as np
import psutil
import os
import ssl
from source_tracker import SourceTracker, StreamLogger, JsonFormatter, ImageRelevanceFilter

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
class ParallelProcessor:
    def __init__(self):
        self.content_hashes = set()
        self.chunk_size = 1000  # tokens per chunk
        self.max_workers = 5
        
    def hash_content(self, content: str) -> str:
        """Generate hash for content deduplication"""
        return hashlib.md5(content.encode()).hexdigest()

    def is_duplicate(self, content: str) -> bool:
        """Check if content is duplicate"""
        content_hash = self.hash_content(content)
        if content_hash in self.content_hashes:
            return True
        self.content_hashes.add(content_hash)
        return False

    def create_chunks(self, sections: List[Dict]) -> List[List[Dict]]:
        """Create optimally sized chunks for Gemini API"""
        chunks = []
        current_chunk = []
        current_size = 0
        
        for section in sections:
            section_size = len(str(section))
            if current_size + section_size > self.chunk_size:
                chunks.append(current_chunk)
                current_chunk = [section]
                current_size = section_size
            else:
                current_chunk.append(section)
                current_size += section_size
                
        if current_chunk:
            chunks.append(current_chunk)
        return chunks
    
class SourceManager:
    """Manages source tracking and indexing"""
    def __init__(self):
        self.source_map = {}  # url -> index
        self.source_index = 1
        self.content_sources = {}  # content_hash -> source_index

    def add_source(self, url: str) -> int:
        if url not in self.source_map:
            self.source_map[url] = self.source_index
            self.source_index += 1
        return self.source_map[url]

    def get_source_index(self, url: str) -> int:
        return self.source_map.get(url, 0)

    def get_source_list(self) -> List[str]:
        return [url for url, _ in sorted(self.source_map.items(), key=lambda x: x[1])]

class MultiUrlProcessor:
    def __init__(self, google_api_key: str, search_engine_id: str, stream_logger: Optional[StreamLogger] = None):
        self.search_service = build('customsearch', 'v1', developerKey=google_api_key)
        self.search_engine_id = search_engine_id
        self.summarizer = AdvancedWebSummarizer()
        self.session = None
        self.cache = {}
        self.parallel_processor = ParallelProcessor()
        self.formatter = JsonFormatter()
        self.stream_logger = stream_logger
        self.timeout = 3  # 5 second timeout


        self.blocked_domains = [
            'youtube.com',
            'youtu.be',
            'youtube-nocookie.com'
        ]

        self.stats = {
            'urls_processed': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'total_chunks': 0,
            'processing_times': []
        }
        logging.info("Initializing MultiUrlProcessor")
        self.summarizer = AdvancedWebSummarizer()
        self.config = PerformanceConfig()
        self.cache_manager = CacheManager()
        self.connection_pool = ConnectionPool(self.config)
        self.executor = ThreadPoolExecutor(max_workers=self.config.MAX_WORKERS)
        self.process_pool = ProcessPoolExecutor(max_workers=self.config.MAX_WORKERS)
        self.metrics = PerformanceMetrics(start_time=time.time())
        self.cache = TwoLevelCache()
        self.semaphore = asyncio.Semaphore(50)  # Increased from 20
        genai.configure(api_key='AIzaSyDPAcNKn9ljNR1F8eI8i6ViwofeL9CiKBw')
        self.model = genai.GenerativeModel('gemini-1.5-flash')
        
        self.batch_processor = BatchProcessor(
            max_concurrent=50,  # Increased from 20
            rate_limit=20  # Increased from 10
        )
        self.source_manager = SourceManager()

    def _is_blocked_url(self, url: str) -> bool:
            """Check if URL is from blocked domain"""
            return any(domain in url.lower() for domain in self.blocked_domains)
        
    async def _stream_gemini_response(self, response):
        """Async generator wrapper for Gemini response"""
        for chunk in response:
            if chunk and chunk.text:
                yield chunk.text
            await asyncio.sleep(0.1)  # Prevent blocking

    async def generate_streaming_chunks(
        self,
        sections: List[Dict],
        query: str = "",
        log_queue: Optional[asyncio.Queue] = None,
        template: Optional[Callable] = None
    ) -> AsyncGenerator[str, None]:
        try:
            prompt_content = self.summarizer._format_sections_for_prompt(sections)
            if not prompt_content:
                yield "Error: No content to summarize"
                return

            prompt = template(prompt_content, query) if template else prompt_content
            
            # Get response from Gemini
            response = self.model.generate_content(
                prompt,
                generation_config={
                    "temperature": 0.5,
                    "top_p": 0.9,
                    "top_k": 40,
                    "max_output_tokens": 2048
                },
                stream=True
            )

            # Stream chunks with async wrapper
            async for chunk in self._stream_gemini_response(response):
                yield chunk
                if log_queue:
                    await log_queue.put({"type": "chunk", "content": chunk})

        except Exception as e:
            error_msg = f"Error generating summary: {str(e)}"
            logging.error(error_msg)
            yield error_msg
        

    async def init_session(self):
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self.session = aiohttp.ClientSession(timeout=timeout)

    async def close_session(self):
        if self.session:
            await self.session.close()
            self.session = None

    @lru_cache(maxsize=100)
    def search_google(self, query: str, num_results: int = 10) -> List[str]:
        try:
            results = self.search_service.cse().list(
                q=query,
                cx=self.search_engine_id,
                num=num_results
            ).execute()

            filtered_urls = [
                item['link'] for item in results.get('items', [])
                if not self._is_blocked_url(item['link'])
            ]

            logging.info(f"Filtered {len(results.get('items', [])) - len(filtered_urls)} blocked URLs")
            return filtered_urls
        except Exception as e:
            print(f"Search error: {e}")
            return []

    @sleep_and_retry
    @limits(calls=30, period=60)
    async def _rate_limited_request(self, url: str):
        session = await self.connection_pool.get_session()
        async with session.get(url) as response:
            return await response.text()

    async def process_images_parallel(self, images: List[Dict]) -> List[Dict]:
        def process_single_image(img_data):
            try:
                # Download and process image
                response = requests.get(img_data['url'], timeout=5)
                img = PIL.Image.open(io.BytesIO(response.content))
                
                # Basic image processing
                img = img.convert('RGB')
                img.thumbnail((800, 800))  # Resize if too large
                
                # Calculate image features
                img_array = np.array(img)
                avg_color = img_array.mean(axis=(0, 1))
                
                # Update image metadata
                img_data['processed'] = True
                img_data['dimensions'] = img.size
                img_data['avg_color'] = avg_color.tolist()
                
                return img_data
            except Exception as e:
                logging.error(f"Error processing image {img_data['url']}: {str(e)}")
                return None

        if not images:
            return []

        # Process images in parallel using process pool
        loop = asyncio.get_event_loop()
        processed_images = []
        
        # Process in batches to manage memory
        batch_size = self.config.BATCH_SIZE
        for i in range(0, len(images), batch_size):
            batch = images[i:i + batch_size]
            
            # Check memory usage before processing
            if psutil.virtual_memory().percent > (100 - self.config.MIN_MEMORY_PCT):
                gc.collect()  # Force garbage collection
                await asyncio.sleep(1)  # Wait for memory to free up
            
            # Process batch
            future_to_img = {
                loop.run_in_executor(self.process_pool, process_single_image, img): img
                for img in batch
            }
            
            for future in asyncio.as_completed(future_to_img):
                result = await future
                if result:
                    processed_images.append(result)

        return processed_images

    async def process_urls(self, query: str, log_queue: Optional[asyncio.Queue] = None) -> Dict:
        # Try to get from cache first
        cache_key = hashlib.md5(query.encode()).hexdigest()
        
        try:
            return await self.cache_manager.get_or_compute(
                cache_key,
                self._process_urls_impl,
                query,
                log_queue
            )
        except Exception as e:
            logging.error(f"Error processing query: {str(e)}")
            return {'error': str(e)}
        finally:
            await self.connection_pool.close()

    async def _process_urls_impl(self, query: str, log_queue: Optional[asyncio.Queue] = None) -> Dict:
        try:
            scraping_start = time.time()
            urls = self.search_google(query)
            if not urls:
                return {
                    'content': {
                        'summary': {'comprehensive': 'No URLs found'},
                        'sections': [],
                        'presentation': [],
                        'sources': [],
                        'images': []
                    },
                    'stats': self.stats,
                    'metrics': self.metrics.to_dict()
                }

            # Process URLs in parallel with batching
            results = await self.process_urls_parallel(urls)
            
            # Calculate total scraping time
            self.metrics.scraping_time = time.time() - scraping_start
            
            if not results:
                return {
                    'content': {
                        'summary': {'comprehensive': 'Failed to process URLs'},
                        'sections': [],
                        'presentation': [],
                        'sources': [],
                        'images': []
                    },
                    'stats': self.stats,
                    'metrics': self.metrics.to_dict()
                }

            # Process and merge results
            merged_content = self.merge_summaries(results)
            
            # Generate comprehensive summary using Gemini
            try:
                gemini_start = time.time()
                summary_result = await self.summarizer.generate_streaming_summary(
                    merged_content.get('section_summaries', []),
                    query,
                    log_queue
                )
                self.metrics.gemini_time = time.time() - gemini_start
                
                if 'error' in summary_result:
                    logging.error(f"Error in summary generation: {summary_result['error']}")
                    summary_result = self.summarizer.generate_fallback_summary(
                        merged_content.get('section_summaries', [])
                    )
                
                # Create final output with summary
                final_output = {
                    'content': {
                        'summary': {
                            'comprehensive': summary_result.get('comprehensive_summary', ''),
                            'sections': merged_content.get('section_summaries', [])
                        },
                        'presentation': summary_result.get('presentation_format', {'slides': []}),
                        'sources': merged_content.get('urls', []),
                        'images': merged_content.get('images', [])
                    },
                    'stats': {
                        **self.stats,
                        'summary_stats': summary_result.get('stats', {}),
                        'timing': {
                            'scraping_time': self.metrics.scraping_time,
                            'gemini_time': self.metrics.gemini_time,
                            'total_time': self.metrics.scraping_time + self.metrics.gemini_time
                        }
                    },
                    'metrics': self.metrics.to_dict()
                }
                
                return final_output

            except Exception as e:
                logging.error(f"Error generating summary: {str(e)}")
                return {
                    'content': {
                        'summary': {'comprehensive': f'Error generating summary: {str(e)}'},
                        'sections': merged_content.get('section_summaries', []),
                        'presentation': {'slides': []},
                        'sources': merged_content.get('urls', []),
                        'images': merged_content.get('images', [])
                    },
                    'stats': self.stats,
                    'metrics': self.metrics.to_dict()
                }

        except Exception as e:
            logging.error(f"Error in process_urls_impl: {str(e)}")
            return {
                'content': {
                    'summary': {'comprehensive': f'Error: {str(e)}'},
                    'sections': [],
                    'presentation': [],
                    'sources': [],
                    'images': []
                },
                'stats': self.stats,
                'metrics': self.metrics.to_dict()
            }

    async def process_chunks(self, chunks: List[List[Dict]]) -> Dict:
        """Process chunks through Gemini API"""
        all_results = []
        for chunk in chunks:
            merged = self.merge_summaries(chunk)
            summary = self.summarizer.generate_comprehensive_summary(merged)
            all_results.append(summary)
            
        return self.merge_final_results(all_results)

    def merge_final_results(self, results: List[Dict]) -> Dict:
        """Merge results from all chunks"""
        merged = {
            'section_summaries': [],
            'comprehensive_summary': '',
            'presentation_format': {'slides': []}
        }
        
        for result in results:
            merged['section_summaries'].extend(result.get('section_summaries', []))
            merged['comprehensive_summary'] += '\n' + result.get('comprehensive_summary', '')
            merged['presentation_format']['slides'].extend(
                result.get('presentation_format', {}).get('slides', [])
            )
            
        return merged

    async def process_single_url(self, url: str, query: str) -> Dict:
        """Process a single URL with improved error handling and content extraction"""
        try:
            logging.info(f"Processing URL: {url}")
            start_time = time.time()
            
            async with self.session.get(url, timeout=self.timeout) as response:
                if response.status != 200:
                    raise aiohttp.ClientError(f"HTTP {response.status}")
                    
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Extract content
                content = {
                    'text': '',
                    'sections': [],
                    'images': self._extract_relevant_images(soup,query=query),
                    'source_url': url  # Add source URL
                }
                
                # Get main content
                main_content = soup.find('main') or soup.find('article') or soup.find('body')
                if main_content:
                    # Extract text
                    content['text'] = main_content.get_text(strip=True)
                    
                    # Extract sections with source tracking
                    sections = []
                    for heading in main_content.find_all(['h1', 'h2', 'h3']):
                        section_content = []
                        current = heading.find_next_sibling()
                        while current and current.name not in ['h1', 'h2', 'h3']:
                            if current.name in ['p', 'div']:
                                section_content.append(current.get_text(strip=True))
                            current = current.find_next_sibling()
                            
                        sections.append({
                            'heading': heading.get_text(strip=True),
                            'content': ' '.join(section_content),
                            'source_url': url  # Add source URL to each section
                        })
                    content['sections'] = sections
                    
                    # Extract images
                    content['images'] = self._extract_relevant_images(soup, query)
                
                processing_time = time.time() - start_time
                self.metrics.add_processing_time(processing_time)
                
                return {
                    'url': url,
                    'content': content,
                    'processing_time': processing_time
                }
                
        except Exception as e:
            logging.error(f"Error processing {url}: {str(e)}")
            return {'error': str(e)}

    def _extract_relevant_images(self, soup: BeautifulSoup, query:str) -> List[Dict]:
        """Extract relevant images from the page"""
        image_filter = ImageRelevanceFilter()
        # images = []
        # for img in soup.find_all('img'):
        #     if img.get('src'):
        #         image_data = {
        #             'url': img['src'],
        #             'alt': img.get('alt', ''),
        #             'title': img.get('title', ''),
        #             'dimensions': {
        #                 'width': img.get('width', ''),
        #                 'height': img.get('height', '')
        #             }
        #         }
        #         images.append(image_data)

        return image_filter.filter_images(soup, query)

    async def process_urls_parallel(self, urls: List[str], query:str) -> List[Dict]:
        """Optimized parallel URL processing with better error handling"""
        if not self.session:
            await self.init_session()
            
        try:
            # Process in larger chunks
            chunk_size = 20
            processed_results = []
            
            for i in range(0, len(urls), chunk_size):
                chunk = urls[i:i + chunk_size]
                tasks = [self.process_single_url(url,query=query) for url in chunk]
                
                try:
                    # Process chunk with timeout
                    chunk_results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Handle results
                    for result in chunk_results:
                        if isinstance(result, dict) and 'error' not in result:
                            processed_results.append({
                                'url': result.get('url', ''),
                                'content': {
                                    'text': result.get('content', {}).get('text', ''),
                                    'sections': result.get('content', {}).get('sections', []),
                                    'images': result.get('content', {}).get('images', [])
                                }
                            })
                            self.stats['successful_extractions'] += 1
                        else:
                            self.stats['failed_extractions'] += 1
                    
                    # Quick memory cleanup
                    if psutil.virtual_memory().percent > 90:
                        gc.collect()
                        await asyncio.sleep(0.05)  # Minimal sleep
                        
                except Exception as chunk_error:
                    logging.error(f"Chunk processing error: {str(chunk_error)}")
                    continue
                    
            self.stats['urls_processed'] += len(urls)
            return processed_results

        except Exception as e:
            logging.error(f"Parallel processing error: {str(e)}")
            return []

    def merge_summaries(self, summaries: List[Dict]) -> Dict:
        """Merge summaries from multiple URLs into a single structured output"""
        if not summaries:
            return {
                'comprehensive_summary': '',
                'section_summaries': [],
                'presentation_format': {'slides': []},
                'urls': set(),
                'images': [],
                'sources': self.source_manager.get_source_list()
            }

        merged = {
            'comprehensive_summary': '',
            'section_summaries': [],
            'presentation_format': {'slides': []},
            'urls': set(),
            'images': [],
            'sources': self.source_manager.get_source_list()
        }

        url_to_index = {}
        current_index = 1

        for summary in summaries:
            if isinstance(summary, dict):
                url = summary.get('url', '')

                if url and url not in url_to_index:
                    url_to_index[url] = current_index
                    current_index += 1

                source_index = self.source_manager.get_source_index(url)
                content = summary.get('content', {})
                
                if isinstance(content, dict):
                    # Handle text content
                    if 'text' in content:
                        merged['comprehensive_summary'] += f"\n[Source {source_index}] {content['text']}"
                    
                    # Handle sections with source URLs
                    sections = content.get('sections', [])
                    for section in sections:
                        if isinstance(section, dict):
                            merged['section_summaries'].append({
                                'heading': section.get('heading', ''),
                                'content': section.get('content', ''),
                                'source_index': source_index,
                                'source_url': url
                            })

                    # Handle images
                    images = content.get('images', [])
                    if images:
                        for image in images:
                            image['source_index'] = source_index
                            image['source_url'] = url
                            merged['images'].append(image)

                    # Track source URL
                    if url:
                        merged['urls'].add(url)

        # Convert URLs set back to list
        merged['urls'] = list(merged['urls'])

        return merged

    async def process_url_with_retry(self, url: str, retries: int = 3) -> Dict:
        for attempt in range(retries):
            try:
                async with self.semaphore:
                    session = await self.connection_pool.get_session()
                    async with session.get(url, timeout=self.timeout) as response:
                        if response.status == 200:
                            return {'url': url, 'content': await response.text()}
                        else:
                            raise aiohttp.ClientError(f"HTTP {response.status}")
            except Exception as e:
                if attempt == retries - 1:
                    logging.error(f"Failed to process {url} after {retries} attempts: {e}")
                    return {'error': str(e)}
                await asyncio.sleep(1 * (attempt + 1))

    async def process_images_batch(self, images: List[Dict]) -> List[Dict]:
        async def process_image_chunk(chunk: List[Dict]) -> List[Dict]:
            with ProcessPoolExecutor() as executor:
                loop = asyncio.get_event_loop()
                tasks = [
                    loop.run_in_executor(executor, self._process_single_image, img)
                    for img in chunk
                ]
                return await asyncio.gather(*tasks)

        processed_images = []
        chunk_size = 10
        for i in range(0, len(images), chunk_size):
            chunk = images[i:i + chunk_size]
            processed_chunk = await process_image_chunk(chunk)
            processed_images.extend([img for img in processed_chunk if img])
            self.metrics.images_processed += len(processed_chunk)
            
        return processed_images

    async def _process_urls_impl(self, query: str, log_queue: Optional[asyncio.Queue] = None) -> Dict:
        try:
            scraping_start = time.time()
            urls = self.search_google(query)
            if not urls:
                return {
                    'content': {
                        'summary': {'comprehensive': 'No URLs found'},
                        'sections': [],
                        'presentation': [],
                        'sources': [],
                        'images': []
                    },
                    'stats': self.stats,
                    'metrics': self.metrics.to_dict()
                }

            # Process URLs in parallel with batching
            results = await self.process_urls_parallel(urls, query)
            
            # Calculate total scraping time
            self.metrics.scraping_time = time.time() - scraping_start
            
            if not results:
                return {
                    'content': {
                        'summary': {'comprehensive': 'Failed to process URLs'},
                        'sections': [],
                        'presentation': [],
                        'sources': [],
                        'images': []
                    },
                    'stats': self.stats,
                    'metrics': self.metrics.to_dict()
                }

            # Process and merge results
            merged_content = self.merge_summaries(results)
            
            # Generate comprehensive summary using Gemini
            try:
                gemini_start = time.time()
                summary_result = await self.summarizer.generate_streaming_summary(
                    merged_content.get('section_summaries', []),
                    query,
                    log_queue
                )
                self.metrics.gemini_time = time.time() - gemini_start
                
                if 'error' in summary_result:
                    logging.error(f"Error in summary generation: {summary_result['error']}")
                    summary_result = self.summarizer.generate_fallback_summary(
                        merged_content.get('section_summaries', [])
                    )
                
                # Create final output with summary
                final_output = {
                    'content': {
                        'summary': {
                            'comprehensive': summary_result.get('comprehensive_summary', ''),
                            'sections': merged_content.get('section_summaries', [])
                        },
                        'presentation': summary_result.get('presentation_format', {'slides': []}),
                        'sources': merged_content.get('urls', []),
                        'images': merged_content.get('images', [])
                    },
                    'stats': {
                        **self.stats,
                        'summary_stats': summary_result.get('stats', {}),
                        'timing': {
                            'scraping_time': self.metrics.scraping_time,
                            'gemini_time': self.metrics.gemini_time,
                            'total_time': self.metrics.scraping_time + self.metrics.gemini_time
                        }
                    },
                    'metrics': self.metrics.to_dict()
                }
                
                return final_output

            except Exception as e:
                logging.error(f"Error generating summary: {str(e)}")
                return {
                    'content': {
                        'summary': {'comprehensive': f'Error generating summary: {str(e)}'},
                        'sections': merged_content.get('section_summaries', []),
                        'presentation': {'slides': []},
                        'sources': merged_content.get('urls', []),
                        'images': merged_content.get('images', [])
                    },
                    'stats': self.stats,
                    'metrics': self.metrics.to_dict()
                }

        except Exception as e:
            logging.error(f"Error in process_urls_impl: {str(e)}")
            return {
                'content': {
                    'summary': {'comprehensive': f'Error: {str(e)}'},
                    'sections': [],
                    'presentation': [],
                    'sources': [],
                    'images': []
                },
                'stats': self.stats,
                'metrics': self.metrics.to_dict()
            }

    def _generate_final_output(self, results: List[Dict]) -> Dict:
        # Combine results and format output
        merged_content = self.merge_summaries(results)
        
        # Compress response if too large
        if sys.getsizeof(merged_content) > 1024 * 1024:  # 1MB
            merged_content = self._compress_content(merged_content)
            
        return {
            'content': merged_content,
            'stats': self.stats,
            'metrics': self.metrics.to_dict()
        }

    def _compress_content(self, content: Dict) -> Dict:
        """Compress large content sections"""
        if 'comprehensive_summary' in content:
            content['comprehensive_summary'] = zlib.compress(
                content['comprehensive_summary'].encode()
            )
        return content
    
    def _create_news_summary_prompt(self, text: str, query: str = "") -> str:
        """Enhanced prompt template specialized for news article summarization with parseable headers"""
        prompt = f"""
        TASK: Generate a factual news summary for: "{query}"

        SOURCE TEXT:
        ###
        {text}
        ###

        REQUIREMENTS:
        - Generate a minimum 200 and maximum 500 words news summary
        - Include only verified facts and direct quotes
        - Exclude personal opinions, speculation, or analysis
        - Focus on core news elements: what, when, where, who, why
        - Maintain chronological order of events
        - Include only primary sources and official statements
        - Omit background information unless crucial to understanding

        STRUCTURE:

        **SUMMARY**:
        [Core news details]

        **CONCLUSION**:
        [Fact-based conclusion]

        Use neutral, reportorial tone throughout. Exclude all editorial content.
        """
        return prompt.strip()

    # def _create_news_summary_prompt(self, text: str, query: str = "") -> str:
    #     """
    #     Creates a structured prompt for extracting factual news summaries while filtering out opinions and irrelevant details.
        
    #     Args:
    #         text (str): The scraped news article text
    #         query (str): Optional topic or focus area
        
    #     Returns:
    #         str: Formatted prompt template
    #     """
    #     prompt = f"""
    #     TASK: Generate a factual news summary for: "{query}"

    #     SOURCE TEXT:
    #     ###
    #     {text}
    #     ###

    #     REQUIREMENTS:
    #     - Generate a minimum 150-word news summary
    #     - Include only verified facts and direct quotes
    #     - Exclude personal opinions, speculation, or analysis
    #     - Focus on core news elements: what, when, where, who, why
    #     - Maintain chronological order of events
    #     - Include only primary sources and official statements
    #     - Omit background information unless crucial to understanding

    #     STRUCTURE:

    #     1. SUMMARY:
    #     [Core news details]

    #     2. KEY DEVELOPMENTS:
    #     • [Bullet points for major events/updates]

    #     Use neutral, reportorial tone throughout. Exclude all editorial content.
    #     """
    #     return prompt.strip()

    # async def process_single_url_stream(self, url: str, query: str = "", log_queue: Optional[asyncio.Queue] = None) -> Dict:
    #     """
    #     Process a single URL and generate streaming summary
    #     """
    #     try:
    #         logging.info(f"Processing single URL: {url}")
            
    #         # Initialize session if needed
    #         await self.init_session()
            
    #         # Check cache first
    #         cache_key = hashlib.md5(f"{url}:{query}".encode()).hexdigest()
    #         cached_result = await self.cache.get(cache_key)
    #         if cached_result:
    #             self.metrics.cache_hits += 1
    #             return cached_result
                
    #         self.metrics.cache_misses += 1
            
    #         # Process URL
    #         async with self.semaphore:
    #             html_content = await self._rate_limited_request(url)
    #             if not html_content:
    #                 raise ValueError("Failed to fetch URL content")
                
    #             # Extract content using scraper
    #             content = self.summarizer.process_url(url, html_content, query)
    #             if 'error' in content:
    #                 raise ValueError(content['error'])
                
    #             # Generate streaming summary
    #             summary_result = await self.summarizer.generate_streaming_summary(
    #                 content['section_summaries'],
    #                 query,
    #                 log_queue,
    #                 template=self._create_news_summary_prompt
    #             )
                
    #             final_output = {
    #                 'content': {
    #                     'summary': summary_result.get('comprehensive_summary', ''),
    #                     'sections': content['section_summaries'],
    #                     'presentation': summary_result.get('presentation_format', {'slides': []}),
    #                     'sources': [url],
    #                     'images': content.get('images', [])
    #                 },
    #                 'stats': {
    #                     'processing_time': time.time() - self.metrics.start_time,
    #                     'summary_stats': summary_result.get('stats', {})
    #                 },
    #                 'metrics': self.metrics.to_dict()
    #             }
                
    #             # Cache the result
    #             await self.cache.set(cache_key, final_output)
                
    #             return final_output
                
    #     except Exception as e:
    #         logging.error(f"Error processing single URL {url}: {str(e)}")
    #         return {
    #             'error': str(e),
    #             'content': {
    #                 'summary': f"Failed to process URL: {str(e)}",
    #                 'sections': [],
    #                 'presentation': {'slides': []},
    #                 'sources': [url],
    #                 'images': []
    #             }
    #         }
    #     finally:
    #         # Don't close session here as it might be reused
    #         pass

    async def process_single_url_stream(self, url: str, query: str = "", log_queue: Optional[asyncio.Queue] = None) -> AsyncGenerator[str, None]:
        """
        Process a single URL and yield streaming summary chunks
        """
        try:
            logging.info(f"Processing single URL: {url}")
            
            # Initialize session if needed
            await self.init_session()
            
            # Process URL
            async with self.semaphore:
                html_content = await self._rate_limited_request(url)
                if not html_content:
                    raise ValueError("Failed to fetch URL content")
                
                # Extract content using scraper
                content = self.summarizer.process_url(url, html_content, query)
                if 'error' in content:
                    raise ValueError(content['error'])
                
                # Generate and yield streaming summary chunks
                async for chunk in self.generate_streaming_chunks(
                    content['section_summaries'],
                    query,
                    log_queue,
                    template=self._create_news_summary_prompt
                ):
                    yield chunk
                    
        except Exception as e:
            logging.error(f"Error processing single URL {url}: {str(e)}")
            yield f"Error processing URL: {str(e)}"
        finally:
            # Don't close session here as it might be reused
            pass

    def _is_news_url(self, url: str) -> bool:
        """Helper to determine if URL is from a news source"""
        news_domains = [
            'news.', 'reuters.com', 'bloomberg.com', 'apnews.com',
            'bbc.', 'cnn.com', 'nytimes.com', 'wsj.com'
        ]
        return any(domain in url.lower() for domain in news_domains)

class ImprovedWebScraper:
    def __init__(self):
        self.main_content_selectors = [
            'article',
            'main',
            '.main-content',
            '#content',
            '.post-content',
            '[role="main"]',
            '.article-content',
            '.entry-content',
            '#article-body'
        ]

        self.image_containers = [
            '.article-image',
            '.content-image',
            '.figure',
            '.wp-caption',
            'figure',
            '[role="img"]'
        ]

        # Add image scoring weights
        self.image_weights = {
            'location': 0.4,
            'context': 0.3,
            'attributes': 0.2,
            'size': 0.1
        }
        self.source_tracker = SourceTracker()

    def _extract_relevant_images(self, content: BeautifulSoup,query:str) -> List[Dict]:
        """Extract and filter relevant images from content"""
        images = []

        for img in content.find_all('img'):
            score = 0

            # Skip tiny images and ads
            if self._is_ad_image(img):
                continue

            # Score based on location
            if any(img.find_parent(selector) for selector in self.image_containers):
                score += self.image_weights['location']

            # Score based on context
            surrounding_text = self._get_surrounding_text(img)
            if self._has_relevant_context(surrounding_text):
                score += self.image_weights['context']

            # Score based on attributes
            if img.get('alt') or img.get('title'):
                score += self.image_weights['attributes']

            # Score based on size
            if self._has_valid_dimensions(img):
                score += self.image_weights['size']

            # Include image if score meets threshold
            if score >= 0.5:
                images.append({
                    'url': img.get('src', ''),
                    'alt': img.get('alt', ''),
                    'title': img.get('title', ''),
                    'context': surrounding_text[:200],
                    'relevance_score': score
                })

        return images

    def _is_ad_image(self, img) -> bool:
        """Check if image is likely an advertisement"""
        ad_patterns = [
            r'ad[sv]?[_-]',
            r'banner',
            r'promotion',
            r'sponsored',
            r'tracker',
            r'pixel',
            r'analytics'
        ]

        src = img.get('src', '').lower()
        classes = ' '.join(img.get('class', [])).lower()

        return any(re.search(pattern, src) or re.search(pattern, classes)
                  for pattern in ad_patterns)

    def _get_surrounding_text(self, img) -> str:
        """Get text content surrounding the image"""
        prev_sibling = img.find_previous_sibling()
        next_sibling = img.find_next_sibling()

        surrounding = []
        if prev_sibling and prev_sibling.text:
            surrounding.append(prev_sibling.text.strip())
        if next_sibling and next_sibling.text:
            surrounding.append(next_sibling.text.strip())

        return ' '.join(surrounding)

    def _has_relevant_context(self, text: str) -> bool:
        """Check if surrounding text is relevant to article"""
        # Skip if too short
        if len(text.strip()) < 20:
            return False

        # Check for promotional/ad language
        promo_patterns = [
            r'sponsored',
            r'advertisement',
            r'promoted',
            r'buy now',
            r'click here'
        ]

        return not any(re.search(pattern, text, re.I)
                      for pattern in promo_patterns)

    def _has_valid_dimensions(self, img) -> bool:
        """Check if image has reasonable dimensions"""
        try:
            width = int(img.get('width', 0))
            height = int(img.get('height', 0))

            # Skip tiny images
            if width < 100 or height < 100:
                return False

            # Skip extremely wide/tall images
            ratio = width / height if height else 0
            if ratio > 3 or ratio < 0.3:
                return False

            return True

        except (ValueError, ZeroDivisionError):
            return False

    def extract_structured_content(self, url: str,query:str) -> dict:
        try:
            response = requests.get(url, timeout=10,
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # Find main content
            main_content = self._find_main_content(soup)
            if not main_content:
                return {'error': 'Could not find main content'}

            # Extract sections
            sections = self._extract_sections(main_content,query=query)

            # Validate sections
            if not sections:
                return {'error': 'No valid sections found'}

            return {
                'url': url,
                'title': self._extract_title(soup),
                'sections': sections,
                'metadata': self._extract_metadata(soup),
                'images': self._extract_relevant_images(soup,query)
            }

        except Exception as e:
            return {'error': f'Extraction failed: {str(e)}'}

    def _find_main_content(self, soup: BeautifulSoup) -> BeautifulSoup:
        """Find the main content area"""
        for selector in self.main_content_selectors:
            content = soup.select_one(selector)
            if content and len(content.get_text(strip=True)) > 500:
                # Remove unwanted elements
                self._clean_content(content)
                return content
        return None

    def _clean_content(self, content: BeautifulSoup):
        """Remove unwanted elements"""
        unwanted = [
            'script', 'style', 'iframe', 'form',
            '[class*="social"]', '[class*="share"]',
            '[class*="comment"]', '[class*="related"]',
            '[class*="sidebar"]', '[class*="ad-"]'
        ]
        for selector in unwanted:
            for element in content.select(selector):
                element.decompose()

    def _extract_sections(self, content: BeautifulSoup,query:str) -> List[Dict]:
        """Extract content sections with better structure"""
        sections = []
        current_section = {'heading': 'Introduction', 'content': [], 'level': 1}

        for element in content.find_all(['h1', 'h2', 'h3', 'h4', 'p', 'ul', 'ol', 'table']):
            if element.name.startswith('h'):
                if current_section['content']:
                    sections.append(current_section)
                current_section = {
                    'heading': element.get_text(strip=True),
                    'content': [],
                    'level': int(element.name[1])
                }
            else:
                text = self._clean_text(element.get_text())
                content_item = self._process_element(element, content.get('source_url', ''))
                if self._is_relevant(text):
                    url = None
                    if element.name == 'a':
                        url = element.get('href')
                    elif element.find_parent('a'):
                        url = element.find_parent('a').get('href')
                    current_section['content'].append({
                        'type': element.name,
                        'text': text,
                        'url': url if url and url.startswith('http') else None
                    })
                    current_section['content'].append(content_item)

        if current_section['content']:
            sections.append(current_section)

        return sections if sections else [{'heading': 'Content', 'content': [], 'level': 1}]

    def _process_element(self, element: BeautifulSoup, source_url: str) -> Dict:
        """Process different types of elements with source tracking"""
        if element.name == 'table':
            content = self._extract_table(element)
            if self.source_tracker:
                self.source_tracker.add_content_source(str(content), source_url)
            return {
                'type': 'table',
                'content': content,
                'source_url': source_url
            }
        else:
            text = self._clean_text(element.get_text())
            if self._is_relevant(text):
                if self.source_tracker:
                    self.source_tracker.add_content_source(text, source_url)
                return {
                    'type': 'text',
                    'text': text,
                    'source_url': source_url
                }
        return None

    def _extract_table(self, table: BeautifulSoup) -> List[List[str]]:
        """Extract table content as 2D array"""
        table_data = []
        
        # Extract headers
        headers = []
        for th in table.find_all('th'):
            headers.append(th.get_text(strip=True))
        if headers:
            table_data.append(headers)
            
        # Extract rows
        for row in table.find_all('tr'):
            row_data = []
            for cell in row.find_all(['td', 'th']):
                row_data.append(cell.get_text(strip=True))
            if row_data:
                table_data.append(row_data)
                
        return table_data

    def _detect_code_language(self, element: BeautifulSoup) -> str:
        """Detect programming language from code block"""
        classes = element.get('class', [])
        for class_name in classes:
            if 'language-' in class_name:
                return class_name.replace('language-', '')
        return 'unknown'
    def _clean_text(self, text: str) -> str:
        """Clean text content"""
        text = re.sub(r'\s+', ' ', text.strip())
        text = re.sub(r'\n+', '\n', text)
        return text

    def _is_relevant(self, text: str) -> bool:
        """Check content relevance"""
        if len(text.strip()) < 40:
            return False

        # Skip common irrelevant content
        skip_patterns = [
            r'^\d+\s*comments?$',
            r'^share\s*(this|on)',
            r'^posted\s+by',
            r'^copyright\s+\d{4}',
            r'^all\s+rights\s+reserved'
        ]

        return not any(re.search(pattern, text, re.I) for pattern in skip_patterns)

    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract page title"""
        title = soup.find('h1')
        if title:
            return title.get_text(strip=True)
        return soup.title.string if soup.title else ''

    def _extract_metadata(self, soup: BeautifulSoup) -> dict:
        """Extract page metadata"""
        return {
            'author': self._get_meta(soup, ['author', 'article:author']),
            'date': self._get_meta(soup, ['date', 'article:published_time']),
            'description': self._get_meta(soup, ['description'])
        }

    def _get_meta(self, soup: BeautifulSoup, names: List[str]) -> str:
        """Get metadata by possible names"""
        for name in names:
            meta = soup.find('meta', {'name': name}) or soup.find('meta', {'property': name})
            if meta:
                return meta.get('content', '')
        return ''

class AdvancedWebSummarizer:
    def __init__(self):
        #self.section_summarizer = pipeline("summarization")
        # Initialize Gemini API
        genai.configure(api_key='AIzaSyDPAcNKn9ljNR1F8eI8i6ViwofeL9CiKBw')
        self.model = genai.GenerativeModel('gemini-1.5-flash')
        self.scraper = ImprovedWebScraper()
        self.cache_manager = CacheManager()
        self.cache = TwoLevelCache()
        self.metrics = PerformanceMetrics(start_time=time.time())
        self.source_tracker = SourceTracker()

    def is_relevant_content(self, text):
        """
        Determine if content is relevant to main article
        """
        # Filter out common non-content patterns
        irrelevant_patterns = [
            r'\b(ad|advertisement|sponsored|cookie|privacy|login|sign\s*up|follow\s*us|share\s*this|comment|related\s*content)\b',
            r'\b(copyright|terms\s*of\s*service|contact\s*us|newsletter|subscribe)\b',
            r'^(http|www\.|@|©)',
            r'\b(facebook|twitter|instagram|linkedin)\b'
        ]

        # Check if text is too short or matches irrelevant patterns
        if len(text) < 50:
            return False

        # Check against irrelevant patterns
        for pattern in irrelevant_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return False

        return True

    def extract_structured_content(self, url):
        """
        Extract structured content with relevance filtering
        """
        try:
            response = requests.get(url, timeout=10,
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # Remove unnecessary elements
            for script in soup(['script', 'style', 'head', 'nav', 'footer', 'aside']):
                script.decompose()

            heading_tags = ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']

            # Structured content extraction
            content_structure = []
            current_section = None

            for element in soup.find_all(heading_tags + ['p', 'li', 'div', 'span']):
                if element.name in heading_tags:
                    # Start new section
                    if current_section:
                        content_structure.append(current_section)

                    current_section = {
                        'heading': element.get_text(strip=True),
                        'content': []
                    }

                elif current_section and element.name in ['p', 'li', 'div', 'span']:
                    text = element.get_text(strip=True)
                    if text and self.is_relevant_content(text):
                        current_section['content'].append(text)

            # Add last section
            if current_section and current_section['content']:
                content_structure.append(current_section)

            return content_structure

        except Exception as e:
            return {'error': str(e)}

    def summarize_sections(self, content_structure):
      """
      Generate section-specific summaries with URL tracking
      """
      section_summaries = []

      for section in content_structure:
          # Extract text and URLs from content items
          texts = []
          urls = []

          for content_item in section['content']:
              if isinstance(content_item, dict):
                  # Handle structured content
                  texts.append(content_item.get('text', ''))
                  if content_item.get('source_url'):
                      urls.append(content_item['source_url'])
              else:
                  # Handle plain text content
                  texts.append(str(content_item))

          section_summaries.append({
              'heading': section['heading'],
              'summary': ' '.join(texts),
              'urls': list(set(urls)) if urls else []
          })

          logging.info(section_summaries)

      return section_summaries
    
    @sleep_and_retry
    @limits(calls=60, period=60)
    async def generate_streaming_summary(self, section_summaries, query="", log_queue=None):
        if not section_summaries:
            raise ValueError("No content provided for summarization")

        try:
            # Format content for Gemini
            prompt_content = self._format_sections_for_prompt(section_summaries)
            if not prompt_content:
                raise ValueError("Failed to format content for summarization")

            prompt = self._create_summary_prompt(prompt_content, query)
            logging.debug(f"Generated prompt: {prompt[:200]}...")

            # Generate response from Gemini with streaming
            response = self.model.generate_content(
                prompt,
                generation_config={
                    "temperature": 0.5,
                    "top_p": 0.9,
                    "top_k": 40,
                    "max_output_tokens": 2048
                },
                stream=True
            )

            if not response:
                raise ValueError("No response from Gemini API")

            async for chunk in response:
                if chunk and chunk.text:
                    yield chunk.text
                await asyncio.sleep(0.1)  # Add small delay between chunks

        except Exception as e:
            logging.error(f"Error in generate_streaming_summary: {str(e)}")
            raise ValueError(f"Summary generation failed: {str(e)}")

    def _format_sections_for_prompt(self, sections: List[Dict]) -> str:
        if not sections:
            return ""

        formatted_text = []
        
        # Add source reference list
        sources = {
            section.get('source_url'): idx + 1 
            for idx, section in enumerate(sections) 
            if section.get('source_url')
        }
        
        if sources:
            formatted_text.append("SOURCES:")
            for url, idx in sources.items():
                formatted_text.append(f"[{idx}] {url}")
        
        formatted_text.append("\nCONTENT:")
        
        # Format sections with source references
        for section in sections:
            heading = section.get('heading', 'Untitled Section')
            content = section.get('content') or section.get('summary', '')
            source_idx = sources.get(section.get('source_url'), '?')
            
            if content:
                formatted_text.append(f"\nSECTION: {heading} [Source {source_idx}]")
                formatted_text.append(str(content))
        
        return "\n".join(formatted_text)

    @sleep_and_retry
    @limits(calls=60, period=60)
    async def generate_streaming_summary(
        self, 
        section_summaries: List[Dict],
        query: str = "",
        log_queue: Optional[asyncio.Queue] = None,
        template: Optional[Callable] = None
    ) -> Dict:
        try:
            logging.info(f"Starting summary generation with {len(section_summaries)} sections")
            
            # Validate and clean sections
            valid_sections = []
            for section in section_summaries:
                logging.debug(f"Processing section: {section}")
                
                # Check if section has required content
                if isinstance(section, dict):
                    # Handle both content and summary fields
                    content = None
                    if 'content' in section and section['content']:
                        if isinstance(section['content'], list):
                            content = ' '.join(str(c) for c in section['content'])
                        else:
                            content = str(section['content'])
                    elif 'summary' in section and section['summary']:
                        content = str(section['summary'])

                    if content and section.get('heading'):
                        cleaned_content = self._clean_section_content(content)
                        if cleaned_content:
                            valid_sections.append({
                                'heading': section['heading'],
                                'content': cleaned_content,
                                'source_url': section.get('source_url', section.get('urls', []))
                            })
                            logging.debug(f"Added valid section: {valid_sections[-1]}")

            logging.info(f"Found {len(valid_sections)} valid sections")

            if not valid_sections:
                logging.warning("No valid sections found for summarization")
                return {
                    'comprehensive_summary': 'No valid content found to summarize. Please check the URL content.',
                    'presentation_format': {'slides': []},
                    'stats': {'processing_time': 0, 'chunk_count': 0}
                }

            # Format content for Gemini
            prompt_content = self._format_sections_for_prompt(valid_sections)
            logging.debug(f"Generated prompt content: {prompt_content}")

            # Use provided template or default
            prompt_creator = template if template else self._create_summary_prompt
            prompt = prompt_creator(prompt_content, query)
            logging.debug(f"Final prompt: {prompt}")

            # Generate summary
            start_time = time.time()
            try:
                logging.info("Sending request to Gemini API")
                response = self.model.generate_content(
                    prompt,
                    generation_config={
                        "temperature": 0.5,
                        "top_p": 0.9,
                        "top_k": 40,
                        "max_output_tokens": 2048
                    }
                )
                
                if not response or not response.text:
                    raise ValueError("Empty response from Gemini API")
                
                summary_text = response.text
                slides = self.parse_presentation_format(summary_text)
                
                processing_time = time.time() - start_time
                self.metrics.gemini_time = processing_time
                
                logging.info(f"Summary generation completed in {processing_time:.2f}s")
                
                return {
                    'comprehensive_summary': summary_text,
                    'presentation_format': {'slides': slides},
                    'stats': {
                        'processing_time': processing_time,
                        'chunk_count': 1,
                        'sections_processed': len(valid_sections)
                    }
                }
                
            except Exception as e:
                logging.error(f"Error in Gemini API call: {str(e)}")
                raise

        except Exception as e:
            logging.error(f"Error in generate_streaming_summary: {str(e)}")
            return self.generate_fallback_summary(section_summaries)

    def _clean_section_content(self, content: Union[str, List[str], Dict]) -> str:
        """Clean and format section content"""
        if isinstance(content, str):
            return content.strip()
        elif isinstance(content, list):
            return ' '.join(str(item).strip() for item in content if item)
        elif isinstance(content, dict):
            return content.get('text', '').strip()
        return ''


    def _create_summary_prompt(self, text: str, query: str = "") -> str:
        """Enhanced prompt that emphasizes source attribution"""
        prompt = f"""
        Task: Create a comprehensive summary from the provided content{' addressing: ' + query if query else ''}.
        Important: Sources are listed at the top. Reference them using [Source X] notation.
        
        INSTRUCTIONS:
        - Every fact must include a source reference [Source X]
        - Cross-reference information between sources
        - Note any contradictions between sources
        - Use multiple sources to validate key points
        - Maintain clear source attribution throughout

        {text}
        
        FORMAT YOUR RESPONSE WITH SOURCE REFERENCES:
        
        TITLE:
        [Topic title]
        
        KEY FACTS:
        1. [Fact 1 synthesized from multiple sources] [Source X, Y]
        2. [Fact 2 synthesized from multiple sources] [Source X, Z] 
        3. [Fact 3 synthesized from multiple sources] [Source Y, Z]

        MAIN POINTS:
        • [Point 1 drawing from multiple sources] [Source X, Y]
        • [Point 2 drawing from multiple sources] [Source Y, Z]
        • [Point 3 drawing from multiple sources] [Source X, Z]

        DETAILED ANALYSIS:
        [Comprehensive analysis comparing and contrasting all sources]
        [Source X, Y, Z]

        CONCLUSION:
        [Synthesized conclusion incorporating insights from all sources]

        REFERENCES:
        1. [Source 1 URL]
        2. [Source 2 URL]
        3. [Source 3 URL]

        STRICT FORMATTING RULES:
        - Keep exactly these section headers in UPPERCASE
        - Use bullet points (•) for MAIN POINTS
        - Number all KEY FACTS and REFERENCES
        - Include [Source X] citations
        - Do not add any other sections
        - Do not modify the format
        """
        return prompt.strip()

    def parse_presentation_format(self, text: str) -> List[Dict]:
        """Parse response with strict section matching"""
        slides = []
        sections = {
            'TITLE': {'title': 'Title', 'points': []},
            'KEY FACTS': {'title': 'Key Facts', 'points': []},
            'MAIN POINTS': {'title': 'Main Points', 'points': []},
            'DETAILED ANALYSIS': {'title': 'Detailed Analysis', 'points': []},
            'CONCLUSION': {'title': 'Conclusion', 'points': []},
            'REFERENCES': {'title': 'References', 'points': []}
        }
        
        current_section = None
        
        for line in text.split('\n'):
            line = line.strip()
            if not line:
                continue
                
            # Check for section headers
            if line in sections:
                current_section = sections[line]
                slides.append(current_section)
                continue
                
            # Add content to current section
            if current_section:
                # Handle numbered/bulleted points
                if line.startswith(('•', '-', '1.', '2.', '3.')):
                    current_section['points'].append(line.lstrip('•-123. '))
                else:
                    current_section['points'].append(line)
                    
        # Filter out empty sections
        return [slide for slide in slides if slide['points']]
    
    def generate_fallback_summary(self, filtered_summaries: List[Dict]) -> Dict:
        """
        Generate a basic structured summary as fallback
        """
        combined_text = " ".join([
            f"{section['heading']}: {section['summary']}"
            for section in filtered_summaries
        ])

        return {
            'section_summaries': filtered_summaries,
            'comprehensive_summary': combined_text[:500],
            'content_category': 'general',
            'presentation_format': {
                'slides': [
                    {
                        'title': 'Overview',
                        'points': [combined_text[:200]]
                    }
                ]
            }
        }

    def process_url(self, url: str, html_content: str = None, query: str = "") -> Dict:
        try:
            # Get content structure
            base_url = '/'.join(url.split('/')[:3])

            if html_content is None:
                content_structure = self.scraper.extract_structured_content(url, query)
            else:
                soup = BeautifulSoup(html_content, 'html.parser')
                content_structure = {
                    'title': self.scraper._extract_title(soup),
                    'sections': self.scraper._extract_sections(soup, query),
                    'metadata': self.scraper._extract_metadata(soup),
                }

            # Validate structure
            if not content_structure or not content_structure.get('sections'):
                return {'error': f'No content found for {url}'}

            # Process sections
            processed_sections = []
            for section in content_structure['sections']:
                # Extract text and validate
                if not section.get('content'):
                    continue
                
                texts = []
                for item in section['content']:
                    if isinstance(item, dict):
                        text = item.get('text', '').strip()
                        if text:
                            texts.append(text)
                    elif isinstance(item, str) and item.strip():
                        texts.append(item.strip())

                if texts:
                    processed_sections.append({
                        'heading': section.get('heading', 'Section'),
                        'content': texts,
                        'source_url': url
                    })

            # Generate section summaries
            section_summaries = []
            for section in processed_sections:
                combined_text = ' '.join(section['content'])
                if len(combined_text) > 50:  # Minimum length check
                    section_summaries.append({
                        'heading': section['heading'],
                        'summary': combined_text,
                        'urls': section['source_url'],
                        'images': section.get('images', [])
                    })
            
            logging.info(f"=============================\n{section_summaries}\n=============================")

            return {
                'url': url,
                'title': content_structure.get('title', ''),
                'section_summaries': section_summaries,
                'images': content_structure.get('images', [])
            }

        except Exception as e:
            print(f"Error processing {url}: {e}")
            return {'error': str(e)}

    def _process_section_content(self, section: Dict, url: str) -> Dict:
        """Helper method to process section content"""
        processed_content = []
        for item in section.get('content', []):
            if isinstance(item, dict):
                processed_item = self._process_content_item(item, url)
                if processed_item:
                    processed_content.append(processed_item)
            elif str(item).strip():
                processed_content.append({
                    'text': str(item),
                    'source_url': url,
                    'type': 'text'
                })

        if processed_content:
            return {
                'heading': section.get('heading', 'Untitled Section'),
                'content': processed_content
            }
        return None

class PerformanceConfig:
    def __init__(self):
        # Increased parallel processing limits
        self.RATE_LIMIT = 50  # Increased from 30
        self.MAX_CONNECTIONS = 50  # Increased from 20
        self.CACHE_TTL = 3600
        self.MAX_CACHE_SIZE = 1000
        self.BATCH_SIZE = 20  # Increased from 10
        self.MAX_WORKERS = min(32, (psutil.cpu_count(logical=False) or 2) * 2)
        self.MIN_MEMORY_PCT = 10  # Reduced from 20
        self.DNS_CACHE_TTL = 600  # Increased to 10 minutes
        self.KEEPALIVE_TIMEOUT = 15  # Reduced from 30

class CacheManager:
    def __init__(self):
        self.query_cache = TTLCache(maxsize=1000, ttl=3600)  # 1 hour TTL
        self.image_cache = LRUCache(maxsize=100)  # LRU cache for processed images
        self.content_cache = TTLCache(maxsize=500, ttl=1800)  # 30 minutes TTL

    async def get_or_compute(self, key, compute_func, *args, **kwargs):
        if key in self.query_cache:
            return self.query_cache[key]
        
        result = await compute_func(*args, **kwargs)
        self.query_cache[key] = result
        return result

class ConnectionPool:
    def __init__(self, config: PerformanceConfig):
        self.config = config
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        
        # Optimized connector settings
        self.connector = TCPConnector(
            limit=config.MAX_CONNECTIONS,
            ttl_dns_cache=config.DNS_CACHE_TTL,
            force_close=False,
            enable_cleanup_closed=True,
            ssl=False,  # Disable SSL verification
            keepalive_timeout=config.KEEPALIVE_TIMEOUT,
            limit_per_host=20,  # Increased from 10
            use_dns_cache=True
        )
        
        # Reduced timeouts
        self.timeout = ClientTimeout(
            total=5,      # Reduced from 30
            connect=2,    # Reduced from 10
            sock_connect=2,
            sock_read=5
        )
        
        self.session = None
        self.headers = {
            'Accept-Encoding': 'gzip, deflate',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Connection': 'keep-alive'
        }

    async def get_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession(
                connector=self.connector,
                timeout=self.timeout,
                headers=self.headers,
                trust_env=True,
                cookie_jar=aiohttp.DummyCookieJar(),
                auto_decompress=True,
                raise_for_status=False  # Don't raise exceptions for HTTP errors
            )
        return self.session
    

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None

def monitor_memory(func):
    async def wrapper(*args, **kwargs):
        if psutil.virtual_memory().percent > (100 - self.config.MIN_MEMORY_PCT):
            gc.collect()
            await asyncio.sleep(1)
        return await func(*args, **kwargs)
    return wrapper

# Add new performance monitoring class
@dataclass
class PerformanceMetrics:
    start_time: float
    urls_processed: int = 0
    images_processed: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    memory_usage: float = 0.0
    processing_times: List[float] = None
    scraping_time: float = 0.0  # Added for scraping time
    gemini_time: float = 0.0    # Added for Gemini processing time

    def __post_init__(self):
        self.processing_times = []

    def add_processing_time(self, duration: float):
        self.processing_times.append(duration)

    def get_average_time(self) -> float:
        return sum(self.processing_times) / len(self.processing_times) if self.processing_times else 0

    def to_dict(self) -> Dict:
        return {
            'total_time': time.time() - self.start_time,
            'urls_processed': self.urls_processed,
            'images_processed': self.images_processed,
            'cache_hits': self.cache_hits,
            'cache_misses': self.cache_misses,
            'avg_processing_time': self.get_average_time(),
            'memory_usage_mb': self.memory_usage / (1024 * 1024),
            'scraping_time_seconds': self.scraping_time,
            'gemini_time_seconds': self.gemini_time
        }

# Update CacheManager with two-level caching
class TwoLevelCache:
    def __init__(self, cache_dir: str = '.cache'):
        self.memory_cache = TTLCache(maxsize=1000, ttl=3*3600)
        self.disk_cache = diskcache.Cache(cache_dir)
        
    async def get(self, key: str) -> Optional[Any]:
        try:
            # Try memory cache first
            if key in self.memory_cache:
                value = self.memory_cache[key]
                return self._decompress_value(value)
                
            # Try disk cache
            if key in self.disk_cache:
                value = self.disk_cache[key]
                decompressed = self._decompress_value(value)
                # Update memory cache with decompressed value
                self.memory_cache[key] = decompressed
                return decompressed
                
            return None
            
        except Exception as e:
            logging.error(f"Cache get error: {str(e)}")
            return None
        
    async def set(self, key: str, value: Any):
        try:
            compressed = self._compress_value(value)
            self.memory_cache[key] = value  # Store uncompressed in memory
            self.disk_cache[key] = compressed  # Store compressed on disk
        except Exception as e:
            logging.error(f"Cache set error: {str(e)}")

    def _compress_value(self, value: Any) -> bytes:
        """Compress dictionary or list to bytes"""
        if isinstance(value, (dict, list)):
            return zlib.compress(json.dumps(value).encode())
        return value

    def _decompress_value(self, value: Any) -> Any:
        """Decompress bytes back to dictionary or list"""
        if isinstance(value, bytes):
            try:
                return json.loads(zlib.decompress(value).decode())
            except:
                return value
        return value

class BatchProcessor:
    def __init__(self, max_concurrent: int = 50, rate_limit: int = 20):
        self.max_concurrent = max_concurrent
        self.rate_limit = rate_limit
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.last_request_time = 0
        self.active_tasks = set()
        self.stopping = False
        self.batch_size = 20  # Increased from 10

    async def process_batch(self, tasks: List[Callable], *args, **kwargs) -> List[Any]:
        """Optimized batch processing with better error handling"""
        results = []
        
        for i in range(0, len(tasks), self.batch_size):
            batch = tasks[i:i + self.batch_size]
            batch_tasks = []
            
            for task in batch:
                if asyncio.iscoroutine(task):
                    batch_tasks.append(self._create_task(self._execute_with_rate_limit(task)))
                else:
                    batch_tasks.append(self._create_task(
                        self._execute_with_rate_limit(task, *args, **kwargs)))
            
            try:
                # Process batch with timeout
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                results.extend([r for r in batch_results if r is not None])
                
                # Minimal delay between batches
                await asyncio.sleep(0.05)
                
            except Exception as e:
                logging.error(f"Batch error: {str(e)}")
                continue
            
        return results

    def _create_task(self, coro) -> asyncio.Task:
        """Create and track an asyncio task"""
        task = asyncio.create_task(coro)
        self.active_tasks.add(task)
        task.add_done_callback(self.active_tasks.discard)
        return task

    async def _execute_with_rate_limit(self, task: Union[Callable, Awaitable], *args, **kwargs) -> Any:
        """Execute a single task with rate limiting"""
        try:
            async with self.semaphore:
                if self.stopping:
                    raise asyncio.CancelledError()

                # Apply rate limiting
                current_time = time.time()
                time_since_last = current_time - self.last_request_time
                if time_since_last < (1 / self.rate_limit):
                    await asyncio.sleep((1 / self.rate_limit) - time_since_last)
                
                self.last_request_time = time.time()
                
                # Handle different types of tasks
                if asyncio.iscoroutine(task):
                    return await task
                elif asyncio.iscoroutinefunction(task):
                    return await task(*args, **kwargs)
                else:
                    return await asyncio.to_thread(task, *args, **kwargs)

        except asyncio.CancelledError:
            logging.info("Task cancelled")
            raise
        except Exception as e:
            logging.error(f"Task execution error: {str(e)}")
            raise

    async def cancel_all_tasks(self):
        """Cancel all active tasks"""
        self.stopping = True
        if self.active_tasks:
            for task in self.active_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*self.active_tasks, return_exceptions=True)
        self.active_tasks.clear()

    async def shutdown(self):
        """Gracefully shutdown the batch processor"""
        self.stopping = True
        await self.cancel_all_tasks()


