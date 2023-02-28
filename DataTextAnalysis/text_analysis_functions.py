# Databricks notebook source
# MAGIC %pip install nltk

# COMMAND ----------

import string
import nltk
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('punkt')
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk import pos_tag

nltk.download('averaged_perceptron_tagger')

def remove_stopwords(text: str) -> str:
    """
    Removes stopwords from the given text.

    Args:
        text: The input string text string from which stopwords need to be removed.

    Returns:
        string: The modified text string with stopwords removed.
    """
    
    stopwords_english = set(stopwords.words('english'))

    # Split text string into individual words.
    words = text.split() #word_tokenize(text)

    # Remove stopwords from the words list.
    words_filtered = [word for word in words if word.lower() not in stopwords_english]

    # Join the filtered words list back into a string.
    text_filtered = ' '.join(words_filtered)

    return text_filtered

def lemmatize_text(text: str) -> str:
    """
    This function lemmatizes the input text by reducing inflectional/derivationally related forms of a word to a common base form. 

    Args:
        text: The string containing the text data.

    Returns:
        string: The string containing the text data with unique lemmatized words.
    """
    
    lemmatizer = WordNetLemmatizer()
    stopwords_english = set(stopwords.words('english'))
    
#     # Remove all punctuation characters
    text = text.translate(str.maketrans('', '', string.punctuation))

    # Tokenize the text into individual words
    words = word_tokenize(text)
    
    # Tag the POS for each word
    tagged_words = pos_tag(words)
    
    # Lemmatize each word based on its POS (part of speech) and remove all punctuation
    lemmatized_words = [lemmatizer.lemmatize(word, pos = get_wordnet_pos(tag))
                       for word, tag in tagged_words]
    
    # Get unique words while maintaining order.
    unique_words = []
    encountered_words = set()
    for word in lemmatized_words:
        if word not in encountered_words:
            encountered_words.add(word)
            unique_words.append(word)
    
    # Join the lemmatized words back into a string.
    text_lemmatized = ' '.join(unique_words)
    
    return text_lemmatized

def get_wordnet_pos(treebank_tag: str) -> str:
    """
    Maps POS (part of speech) tag to first character used by WordNetLemmatizer.
    
    Args
        treebank_tag: POS tag from the Treebank corpus.
        
    Returns
        First character corresponding to POS in WordNetLemmatizer (n, v, a, r).
    """
    
    pos_mapping = {'N': 'n'  # noun
                 , 'V': 'v'  # verb
                 , 'J': 'a'  # adjective
                 , 'R': 'r'  # adverb
               }
    pos = treebank_tag[0].upper()
    default_pos_mapping = 'n'
    return pos_mapping.get(pos, default_pos_mapping)
