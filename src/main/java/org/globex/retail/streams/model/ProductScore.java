package org.globex.retail.streams.model;

public class ProductScore {

    private String productId;

    private int score;

    private String category;

    public String getProductId() {
        return productId;
    }

    public int getScore() {
        return score;
    }

    public String getCategory() {
        return category;
    }

    public static ProductScore sum(ProductScore p1, ProductScore p2) {
        Builder builder = newBuilder(p1);
        builder.category(p1.getCategory()).score(p1.getScore() + p2.getScore());
        return builder.build();
    }

    public static Builder newBuilder(ProductScore productLikes) {
        return new Builder(productLikes.getProductId())
                .category(productLikes.getCategory())
                .score(productLikes.getScore());
    }

    public static class Builder {

        private final ProductScore productScore;

        public Builder(String productId) {
            productScore = new ProductScore();
            productScore.productId = productId;
            productScore.category = "product";
            productScore.score = 1;
        }

        public Builder score(int score) {
            productScore.score = score;
            return this;
        }

        public Builder category(String category) {
            productScore.category = category;
            return this;
        }

        public ProductScore build() {
            return productScore;
        }

    }
}
