"""Generate realistic mock YouTuber profiles for pipeline testing."""

import json
import random

from faker import Faker

fake = Faker()

# Templates per category — realistic channel descriptions and video titles
CREATOR_TEMPLATES = {
    "Technology": {
        "descriptions": [
            "Tech reviewer covering the latest smartphones, laptops, and gadgets. New videos every week!",
            "Your go-to channel for honest tech reviews, unboxings, and comparisons.",
            "I break down complex tech into simple, understandable content. PC builds, software tips, and more.",
            "AI and tech enthusiast. Exploring how technology shapes our future.",
        ],
        "video_templates": [
            "{product} Review: Is It Worth It in 2024?",
            "Top 5 {product} You NEED in 2024",
            "{product} vs {product2}: Which One Should You Buy?",
            "I Switched to {product} for 30 Days...",
            "The TRUTH About {product}",
            "Unboxing the NEW {product} - First Impressions",
            "{product} Setup Tour 2024",
        ],
        "products": [
            "iPhone 16", "Galaxy S24", "MacBook Pro", "iPad Air", "RTX 4090",
            "Steam Deck", "Apple Watch", "AirPods Pro", "Sony WH-1000XM5",
            "Pixel 9", "M4 Mac Mini", "Nothing Phone", "Framework Laptop",
        ],
    },
    "Entertainment": {
        "descriptions": [
            "Gaming content creator. Let's plays, reviews, and esports commentary.",
            "Movie buff sharing reviews, rankings, and deep dives into cinema.",
            "Comedy sketches and funny moments. Subscribe for daily laughs!",
            "ASMR artist creating relaxing content for better sleep and focus.",
        ],
        "video_templates": [
            "Playing {game} for the FIRST TIME",
            "{game} is INCREDIBLE - Full Review",
            "Ranking Every {topic} Movie EVER",
            "The Funniest {topic} Compilation 2024",
            "{game} Challenge with Friends!",
            "ASMR {topic} for Deep Sleep",
            "Reacting to {topic} for the First Time",
        ],
        "products": [
            "Elden Ring DLC", "Baldur's Gate 3", "GTA 6 Trailer", "Zelda",
            "Marvel", "Horror", "Minecraft", "Fortnite", "Valorant", "Palworld",
        ],
    },
    "Lifestyle": {
        "descriptions": [
            "Daily vlogs documenting my life in {city}. Travel, food, and good vibes.",
            "Minimalist living. Decluttering, organization tips, and mindful living.",
            "Van life adventures! Exploring the world one road trip at a time.",
            "Pet parent of 3 dogs. Animal content, training tips, and cute moments.",
        ],
        "video_templates": [
            "A Day in My Life in {city}",
            "My Morning Routine 2024",
            "Moving to {city} - First Impressions",
            "Everything I Own as a Minimalist",
            "Van Life: Camping in {location}",
            "My {pet} Does the CUTEST Thing",
            "Room Tour 2024 | Aesthetic Home Decor",
        ],
        "products": [
            "Tokyo", "Seoul", "Taipei", "New York", "London", "Bali",
            "the Mountains", "the Beach", "cat", "dog", "puppy",
        ],
    },
    "Food": {
        "descriptions": [
            "Home chef sharing easy recipes anyone can make. Japanese and Asian cuisine specialist.",
            "Street food hunter exploring night markets and hidden gems around the world.",
            "Baking enthusiast. Cakes, breads, and pastries from scratch.",
            "Mukbang and food challenges. Warning: will make you hungry!",
        ],
        "video_templates": [
            "How to Make Perfect {dish}",
            "{dish} Recipe - Better Than Restaurant!",
            "Street Food Tour in {city} Night Market",
            "I Tried Making {dish} for the First Time",
            "ASMR Mukbang: {dish} Eating Show",
            "{dish} | Easy 15-Minute Recipe",
            "Best {cuisine} Food in {city}",
        ],
        "products": [
            "Ramen", "Sushi", "Dumplings", "Fried Rice", "Pad Thai",
            "Croissant", "Pizza", "Steak", "Curry", "Boba Tea",
            "Taipei", "Tokyo", "Bangkok", "Seoul", "Hong Kong",
        ],
    },
    "Education": {
        "descriptions": [
            "Making science fun and accessible. Complex topics explained simply.",
            "Language learning channel. Learn {lang} with me!",
            "Coding tutorials for beginners and intermediate developers.",
            "Study tips and productivity hacks for students.",
        ],
        "video_templates": [
            "How {topic} Actually Works",
            "{topic} Explained in 10 Minutes",
            "Learn {topic} - Complete Beginner Guide",
            "Why {topic} Will Change Everything",
            "Study With Me | {hours} Hours of Productivity",
            "{topic} Tutorial for Beginners 2024",
            "The History of {topic} - Full Documentary",
        ],
        "products": [
            "Quantum Physics", "Black Holes", "DNA", "Python", "JavaScript",
            "Japanese", "Korean", "Machine Learning", "the Brain", "Evolution",
        ],
    },
    "Finance": {
        "descriptions": [
            "Personal finance tips to help you save money and build wealth.",
            "Stock market analysis and investment strategies for beginners.",
            "Crypto trader sharing daily market analysis and trading tips.",
            "Real estate investor. Property tours, market insights, and investment tips.",
        ],
        "video_templates": [
            "How I Save ${amount} Every Month",
            "{topic} Investment Strategy for 2024",
            "Is {topic} a Good Investment Right Now?",
            "My ${amount} Portfolio Breakdown",
            "How to Start Investing with ${amount}",
            "{topic} Market Analysis - What's Next?",
            "Financial Mistakes to Avoid in Your 20s",
        ],
        "products": [
            "Bitcoin", "Ethereum", "S&P 500", "Real Estate", "Gold",
            "Tesla Stock", "Index Fund", "ETF", "the Housing Market",
        ],
    },
    "Beauty_Fashion": {
        "descriptions": [
            "Makeup artist sharing tutorials, reviews, and beauty tips.",
            "Fashion enthusiast. Outfit ideas, hauls, and styling tips.",
            "Skincare obsessed. Product reviews, routines, and dermatologist-approved tips.",
            "Nail art creator. New designs every week!",
        ],
        "video_templates": [
            "{brand} Full Face Review - Worth the Hype?",
            "My Skincare Routine 2024 | {type} Skin",
            "Fall/Winter Outfit Ideas 2024",
            "Trying {brand} for the First Time",
            "Get Ready With Me | {occasion}",
            "Thrift Haul | Found AMAZING Pieces",
            "{type} Nail Art Tutorial",
        ],
        "products": [
            "Charlotte Tilbury", "Fenty Beauty", "Glossier", "ZARA", "H&M",
            "Oily", "Dry", "Combination", "Date Night", "Work", "Wedding",
        ],
    },
    "Sports_Fitness": {
        "descriptions": [
            "Fitness content creator. Workout routines, nutrition tips, and motivation.",
            "Basketball highlights and training content. Ball is life!",
            "Yoga instructor bringing peace and flexibility to your daily routine.",
            "Running enthusiast training for marathons. Join me on my journey!",
        ],
        "video_templates": [
            "{duration} Minute {type} Workout - No Equipment",
            "My {type} Transformation | Before & After",
            "What I Eat in a Day as an Athlete",
            "Full {type} Workout for Beginners",
            "NBA {topic} Breakdown",
            "I Tried {type} Every Day for 30 Days",
            "Marathon Training Week {num} | Race Prep",
        ],
        "products": [
            "Full Body", "Ab", "Leg", "Arm", "HIIT", "Yoga", "Running",
            "LeBron", "Curry", "Weight Loss", "Muscle Building",
        ],
    },
    "Kids_Family": {
        "descriptions": [
            "Family-friendly content! Toy reviews, crafts, and fun activities for kids.",
            "Parenting tips and family vlogs. Real life with 3 kids.",
            "Educational content for children. Learning through play!",
            "DIY crafts and creative projects the whole family can enjoy.",
        ],
        "video_templates": [
            "{toy} Unboxing and Play!",
            "Learning {topic} with Fun Activities",
            "Family Vlog: Our Trip to {location}",
            "Easy DIY {craft} for Kids",
            "Parenting Tip: How to Handle {topic}",
            "Fun Science Experiments for Kids",
            "A Day with Our {num} Kids",
        ],
        "products": [
            "LEGO", "Play-Doh", "Colors", "Numbers", "ABC", "Dinosaur",
            "Disneyland", "the Zoo", "Paper Airplane", "Slime", "3",
        ],
    },
}

REGIONS = ["TW", "JP", "KR", "US", "HK", "SEA", "Global"]
REGION_WEIGHTS = [0.25, 0.15, 0.10, 0.20, 0.05, 0.10, 0.15]


def _fill_template(template: str, products: list[str]) -> str:
    """Fill template placeholders with random products."""
    result = template
    for placeholder in ["{product}", "{game}", "{dish}", "{city}", "{topic}",
                        "{brand}", "{type}", "{location}", "{pet}", "{lang}",
                        "{cuisine}", "{occasion}", "{craft}", "{toy}"]:
        while placeholder in result:
            result = result.replace(placeholder, random.choice(products), 1)
    # Handle special placeholders
    result = result.replace("{product2}", random.choice(products))
    result = result.replace("{hours}", str(random.randint(3, 10)))
    result = result.replace("{amount}", str(random.choice([100, 500, 1000, 5000, 10000])))
    result = result.replace("{duration}", str(random.choice([10, 15, 20, 30, 45])))
    result = result.replace("{num}", str(random.randint(1, 12)))
    return result


def generate_creator(creator_id: int) -> dict:
    """Generate a single mock creator profile."""
    # Pick 1-2 primary categories (most creators span multiple areas)
    num_categories = random.choices([1, 2], weights=[0.6, 0.4])[0]
    categories = random.sample(list(CREATOR_TEMPLATES.keys()), num_categories)
    primary = categories[0]
    template = CREATOR_TEMPLATES[primary]

    # Generate channel name
    name_styles = [
        f"{fake.first_name()}'s {primary.replace('_', ' ')} Channel",
        f"{fake.first_name()} {fake.last_name()}",
        f"The {primary.replace('_', ' ')} {random.choice(['Show', 'Hub', 'Zone', 'Lab'])}",
        f"{fake.user_name()}_{random.choice(['official', 'tv', 'studio'])}",
    ]
    name = random.choice(name_styles)

    # Generate description
    description = random.choice(template["descriptions"])
    description = description.replace("{city}", random.choice(
        ["Taipei", "Tokyo", "Seoul", "New York", "London", "LA", "Singapore"]))
    description = description.replace("{lang}", random.choice(
        ["Japanese", "Korean", "Chinese", "English", "Spanish"]))

    # Generate video titles (5-8 from primary, maybe 1-2 from secondary)
    num_videos = random.randint(5, 8)
    video_titles = []
    for _ in range(num_videos):
        cat = random.choice(categories)
        t = CREATOR_TEMPLATES[cat]
        title = _fill_template(
            random.choice(t["video_templates"]),
            t["products"],
        )
        video_titles.append(title)

    region = random.choices(REGIONS, weights=REGION_WEIGHTS)[0]

    return {
        "channel_id": f"UC{fake.hexify(text='^^^^^^^^^^^^^^^^^^^^^^')}{creator_id:04d}",
        "name": name,
        "description": description,
        "subscriber_count": random.choice([
            random.randint(1_000, 10_000),       # small
            random.randint(10_000, 100_000),      # mid
            random.randint(100_000, 1_000_000),   # large
            random.randint(1_000_000, 10_000_000), # mega
        ]),
        "region": region,
        "recent_video_titles": video_titles,
        "primary_categories": categories,  # ground truth for evaluation
    }


def generate_dataset(n: int = 500, output_path: str = "data/seed_creators.json"):
    """Generate N mock creator profiles and save to JSON."""
    creators = [generate_creator(i) for i in range(n)]
    with open(output_path, "w") as f:
        json.dump(creators, f, indent=2, ensure_ascii=False)
    print(f"Generated {n} creators → {output_path}")
    return creators


if __name__ == "__main__":
    generate_dataset(500)
