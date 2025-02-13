from typing import Dict, List, Optional, Set, Any
import logging
from datetime import datetime
import socketio
from aiohttp import web
from bs4 import BeautifulSoup


class ImageRelevanceFilter:
    def __init__(self):
        self.min_dimensions = (100, 100)  # Minimum image size
        self.max_dimensions = (3000, 3000) # Maximum image size
        self.min_score = 0.5  # Minimum relevance score
        
        # Scoring weights
        self.weights = {
            'size': 0.2,
            'position': 0.3,
            'context': 0.3,
            'attributes': 0.2
        }
        
        # Irrelevant patterns
        self.irrelevant_patterns = [
            r'advertisement',
            r'banner',
            r'logo',
            r'icon',
            r'avatar',
            r'button',
            r'social',
            r'share',
            r'comment'
        ]

    def is_relevant_to_query(self, img: BeautifulSoup, query: str, context: str) -> bool:
        """Check if image is relevant to search query"""
        query_terms = set(query.lower().split())
        
        # Check alt text
        alt_text = img.get('alt', '').lower()
        if any(term in alt_text for term in query_terms):
            return True
            
        # Check surrounding text
        if any(term in context.lower() for term in query_terms):
            return True
            
        return False

    def get_image_score(self, img: BeautifulSoup, query: str) -> float:
        """Calculate image relevance score"""
        score = 0
        
        # Get image attributes
        src = img.get('src', '')
        alt = img.get('alt', '')
        width = int(img.get('width', 0))
        height = int(img.get('height', 0))
        
        # Skip invalid images
        if not src or any(pattern in src.lower() for pattern in self.irrelevant_patterns):
            return 0
            
        # Size score
        if self.min_dimensions[0] <= width <= self.max_dimensions[0] and \
           self.min_dimensions[1] <= height <= self.max_dimensions[1]:
            score += self.weights['size']
            
        # Position score (higher if in article content)
        if img.find_parent(['article', 'main', '.content']):
            score += self.weights['position']
            
        # Context score
        context = self._get_surrounding_text(img)
        if self.is_relevant_to_query(img, query, context):
            score += self.weights['context']
            
        # Attribute score
        if alt and not any(pattern in alt.lower() for pattern in self.irrelevant_patterns):
            score += self.weights['attributes']
            
        return score

    def _get_surrounding_text(self, img: BeautifulSoup, chars: int = 200) -> str:
        """Get text surrounding the image"""
        context = []
        
        # Get previous text
        prev = img.find_previous(text=True)
        if prev:
            context.append(prev.strip())
            
        # Get next text
        next = img.find_next(text=True)
        if next:
            context.append(next.strip())
            
        return ' '.join(context)[:chars]

    def filter_images(self, soup: BeautifulSoup, query: str) -> List[Dict]:
        """Filter and score images based on relevance"""
        relevant_images = []
        
        for img in soup.find_all('img'):
            score = self.get_image_score(img, query)
            
            if score >= self.min_score:
                image_data = {
                    'url': img.get('src', ''),
                    'alt': img.get('alt', ''),
                    'title': img.get('title', ''),
                    'context': self._get_surrounding_text(img),
                    'relevance_score': score
                }
                relevant_images.append(image_data)
                
        return sorted(relevant_images, key=lambda x: x['relevance_score'], reverse=True)

class SourceTracker:
    def __init__(self):
        self.sources = {}  # Dictionary to track content sources
        self.source_index = 1
        self.content_map = {}  # Map content hash to source

    def add_content_source(self, content: str, source_url: str) -> None:
        """Add a source for a piece of content"""
        if source_url not in self.sources:
            self.sources[source_url] = self.source_index
            self.source_index += 1
        
        content_hash = hash(content)
        self.content_map[content_hash] = source_url

    def get_source_index(self, content: str) -> Optional[int]:
        """Get the source index for a piece of content"""
        content_hash = hash(content)
        source_url = self.content_map.get(content_hash)
        return self.sources.get(source_url)

    def get_source_url(self, content: str) -> Optional[str]:
        """Get the source URL for a piece of content"""
        content_hash = hash(content)
        return self.content_map.get(content_hash)

class StreamLogger:
    def __init__(self):
        self.sio = socketio.AsyncServer(cors_allowed_origins='*')
        self.app = web.Application()
        self.sio.attach(self.app)
        self.setup_routes()

    def setup_routes(self):
        @self.sio.on('connect')
        async def connect(sid, environ):
            print(f"Client connected: {sid}")

    async def emit_log(self, data: Dict[str, Any]):
        await self.sio.emit('log_update', data)

    async def emit_progress(self, current: int, total: int, status: str):
        await self.sio.emit('progress', {
            'current': current,
            'total': total,
            'status': status
        })

class JsonFormatter:
    def __init__(self):
        self.timestamp = datetime.now().isoformat()
        
    def format_content(self, content: Dict, stats: Dict) -> Dict:
        """Format content into structured JSON"""
        images = content.get('images', [])
        logging.info(f"Formatting content with {len(content.get('images', []))} images")
        formatted= {
            "metadata": {
                "generated_at": self.timestamp,
                "processing_stats": stats
            },
            "content": {
                "summary": self._format_summary(content),
                "sections": self._format_sections(content.get('sections', [])),
                "presentation": self._format_presentation(content),
                "sources": self._format_sources(content),
                #"images": self._format_images(images)
            }
        }
        logging.info(f"Formatted output has {len(formatted['content']['images'])} images")
        return formatted
    
    def _format_images(self, images: List[Dict]) -> List[Dict]:
        """Format images data with validation"""
        if not images:
            logging.warning("No images received for formatting")
            return []
            
        formatted_images = []
        for idx, img in enumerate(images):
            logging.info(f"Processing image {idx+1}/{len(images)}")
            
            try:
                # Validate image structure
                if not isinstance(img, dict):
                    logging.warning(f"Invalid image type: {type(img)}")
                    continue
                    
                if 'url' not in img:
                    logging.warning(f"Missing URL in image {idx+1}")
                    continue
                    
                # Normalize URL
                url = img['url']
                if url.startswith('//'):
                    url = f'https:{url}'
                    
                # Create formatted image
                formatted_img = {
                    "url": url,
                    "alt": img.get('alt', ''),
                    "title": img.get('title', ''),
                    "context": img.get('context', ''),
                    "relevance_score": float(img.get('relevance_score', 0))
                }
                
                # Validate URL and score
                if not formatted_img['url'].startswith(('http://', 'https://')):
                    logging.warning(f"Invalid URL format after normalization: {formatted_img['url']}")
                    continue
                    
                if formatted_img['relevance_score'] < 0.3:
                    logging.info(f"Image {idx+1} filtered: low relevance score {formatted_img['relevance_score']}")
                    continue
                    
                formatted_images.append(formatted_img)
                logging.info(f"Added image {idx+1}: {formatted_img['url']}")
                
            except Exception as e:
                logging.error(f"Error processing image {idx+1}: {str(e)}")
                continue
        
        logging.info(f"Successfully formatted {len(formatted_images)} of {len(images)} images")
        return formatted_images

        
    def _format_summary(self, content: Dict) -> Dict:
        return {
            "comprehensive": content.get('comprehensive_summary', ''),
            "sections": self._format_sections(content.get('section_summaries', []))
        }
        
    def _format_sections(self, sections: List[Dict]) -> List[Dict]:
        return [
            {
                "title": section.get('heading', ''),
                "content": section.get('summary', ''),
                "references": section.get('urls', [])
            }
            for section in sections
        ]
        
    def _format_presentation(self, content: Dict) -> List[Dict]:
        return content.get('presentation_format', {}).get('slides', [])
        
    def _format_sources(self, content: Dict) -> List[str]:
        sources = set()
        for section in content.get('section_summaries', []):
            sources.update(section.get('urls', []))
        return list(sources)

