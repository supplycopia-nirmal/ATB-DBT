from html.parser import HTMLParser

class MyHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.depth = 0
        self.tags = []
        self.target_depth = None

    def handle_starttag(self, tag, attrs):
        self.tags.append((tag, dict(attrs).get('id'), dict(attrs).get('class')))
        if dict(attrs).get('id') == 'volume-trend-modal':
            self.target_depth = self.tags[:]
        if tag not in ['img', 'br', 'hr', 'input', 'meta', 'link']:
            self.depth += 1

    def handle_endtag(self, tag):
        if tag not in ['img', 'br', 'hr', 'input', 'meta', 'link']:
            self.depth -= 1
            if self.tags and self.tags[-1][0] == tag:
                self.tags.pop()

parser = MyHTMLParser()
with open('index.html', 'r') as f:
    parser.feed(f.read())
print("Path to modal:")
for t in parser.target_depth:
    print(t)
