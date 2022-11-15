package org.globex.retail.streams.model;

import java.util.Comparator;
import java.util.Objects;

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
        builder.score(p1.getScore() + p2.getScore());
        return builder.build();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (obj instanceof ProductScore) {
            ProductScore productScore = (ProductScore) obj;
            return Objects.equals(this.productId, productScore.productId)
                    && Objects.equals(this.category, productScore.category)
                    && this.score == productScore.score;
        }
        return false;
    }

    public static Builder newBuilder(ProductScore productLikes) {
        return new Builder(productLikes.getProductId())
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

        public ProductScore build() {
            return productScore;
        }

    }

    public static final Comparator<ProductScore> SCORE_ORDER = (pl1, pl2) -> {
        if (pl1.equals(pl2)) {
            return 0;
        }
        int diff = pl2.score - pl1.score;
        if (diff == 0) {
            return -1;
        }
        return diff;
    };
}
