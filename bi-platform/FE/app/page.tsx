"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@/lib/AuthContext";
import { Button } from "@/components/ui/button";
import { 
    BarChart3, 
    Database, 
    LayoutDashboard, 
    ArrowRight, 
    Loader2
} from "lucide-react";

export default function Home() {
    const { isAuthenticated, isLoading, openAuthModal } = useAuth();
    const router = useRouter();

    useEffect(() => {
        if (!isLoading && isAuthenticated) {
            router.push("/hub");
        }
    }, [isAuthenticated, isLoading, router]);

    if (isLoading || isAuthenticated) {
        return (
            <div className="flex h-[80vh] w-full items-center justify-center bg-background">
                <Loader2 className="h-8 w-8 text-primary animate-spin" />
            </div>
        );
    }

    return (
        <div id="root-home-page" className="min-h-[calc(100vh-4rem)] relative bg-background font-sans overflow-hidden flex flex-col justify-center py-12">
            
            {/* Elegant Ambient Glowing Backgrounds */}
            <div className="absolute top-0 w-full h-full overflow-hidden -z-10 pointer-events-none">
                <div className="absolute top-[10%] left-[-10%] w-[50%] h-[50%] rounded-full bg-primary/10 blur-[130px] opacity-70"></div>
                <div className="absolute bottom-[10%] right-[-10%] w-[40%] h-[50%] rounded-full bg-blue-500/5 blur-[130px] opacity-60"></div>
                
                {/* Visual Grid watermark */}
                <svg
                    viewBox="0 0 1400 800"
                    className="absolute inset-0 w-full h-full opacity-[0.06] dark:opacity-[0.04]"
                    preserveAspectRatio="xMidYMid slice"
                    xmlns="http://www.w3.org/2000/svg"
                >
                    {/* Horizontal & Vertical grid lines */}
                    <line x1="0" y1="200" x2="1400" y2="200" stroke="currentColor" strokeWidth="1" />
                    <line x1="0" y1="400" x2="1400" y2="400" stroke="currentColor" strokeWidth="1" />
                    <line x1="0" y1="600" x2="1400" y2="600" stroke="currentColor" strokeWidth="1" />
                    <line x1="350" y1="0" x2="350" y2="800" stroke="currentColor" strokeWidth="1" />
                    <line x1="700" y1="0" x2="700" y2="800" stroke="currentColor" strokeWidth="1" />
                    <line x1="1050" y1="0" x2="1050" y2="800" stroke="currentColor" strokeWidth="1" />
                </svg>
            </div>

            {/* HERO SECTION */}
            <section className="relative px-6 mx-auto max-w-5xl flex flex-col items-center text-center">
                <div className="inline-flex items-center gap-2 px-3 py-1.5 mb-8 rounded-full bg-primary/10 border border-primary/20 text-primary text-sm font-medium animate-in fade-in slide-in-from-bottom-4 duration-500">
                    <span className="flex h-2 w-2 rounded-full bg-primary animate-pulse"></span>
                    Nền tảng BI & Phân tích chứng khoán doanh nghiệp
                </div>

                <h1 className="text-4xl md:text-6xl font-extrabold tracking-tight text-foreground mb-6 max-w-4xl leading-tight">
                    Hệ thống BI & Truy vấn Dữ liệu <span className="text-transparent bg-clip-text bg-gradient-to-r from-primary to-blue-500">Chứng khoán</span> Chuyên nghiệp
                </h1>

                <p className="text-lg md:text-xl text-muted-foreground mb-10 max-w-3xl leading-relaxed">
                    Công cụ quản lý truy vấn SQL, tập hợp dữ liệu thô và trực quan hóa báo cáo phân tích hiệu suất ngành/cổ phiếu vượt trội dành cho các nhà phân tích dữ liệu chuyên nghiệp.
                </p>

                <div className="flex flex-col sm:flex-row gap-4 w-full justify-center mb-16">
                    <Button 
                        size="lg" 
                        className="h-14 px-8 text-base font-semibold shadow-lg shadow-primary/20 hover:scale-105 transition-transform" 
                        onClick={openAuthModal}
                    >
                        Bắt đầu phân tích ngay <ArrowRight className="ml-2 h-5 w-5" />
                    </Button>
                </div>
            </section>

            {/* BI CORE FEATURES SECTION */}
            <section className="py-8 relative">
                <div className="mx-auto max-w-6xl px-6">
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
                        
                        {/* feature 1: data sources */}
                        <div className="p-6 rounded-2xl bg-card/60 backdrop-blur-sm border border-border/50 shadow-sm hover:shadow-md hover:border-primary/20 transition-all duration-300 group">
                            <div className="h-12 w-12 rounded-xl bg-primary/10 flex items-center justify-center text-primary mb-6 group-hover:scale-110 transition-transform">
                                <Database className="h-6 w-6" />
                            </div>
                            <h3 className="text-xl font-bold mb-3">Kho truy vấn dữ liệu</h3>
                            <p className="text-muted-foreground text-sm leading-relaxed">
                                Lưu trữ và thực hiện các câu lệnh SQL/GraphQL truy vấn dữ liệu chứng khoán, chỉ số tài chính thô một cách linh hoạt và tối ưu.
                            </p>
                        </div>

                        {/* feature 2: BI chart builder */}
                        <div className="p-6 rounded-2xl bg-card/60 backdrop-blur-sm border border-border/50 shadow-sm hover:shadow-md hover:border-primary/20 transition-all duration-300 group">
                            <div className="h-12 w-12 rounded-xl bg-blue-500/10 flex items-center justify-center text-blue-500 mb-6 group-hover:scale-110 transition-transform">
                                <BarChart3 className="h-6 w-6" />
                            </div>
                            <h3 className="text-xl font-bold mb-3">Trực quan hóa BI</h3>
                            <p className="text-muted-foreground text-sm leading-relaxed">
                                Biến đổi dữ liệu thô thành biểu đồ động (cột, đường, tròn, bản đồ nhiệt) trong tích tắc với các tùy chọn kéo thả và lọc nâng cao.
                            </p>
                        </div>

                        {/* feature 3: Dashboard management */}
                        <div className="p-6 rounded-2xl bg-card/60 backdrop-blur-sm border border-border/50 shadow-sm hover:shadow-md hover:border-primary/20 transition-all duration-300 group">
                            <div className="h-12 w-12 rounded-xl bg-emerald-500/10 flex items-center justify-center text-emerald-500 mb-6 group-hover:scale-110 transition-transform">
                                <LayoutDashboard className="h-6 w-6" />
                            </div>
                            <h3 className="text-xl font-bold mb-3">Dashboard đầu tư</h3>
                            <p className="text-muted-foreground text-sm leading-relaxed">
                                Tổng hợp nhiều biểu đồ thành một trang báo cáo BI duy nhất nhằm giám sát hoạt động, dòng tiền và sức khỏe tài chính doanh nghiệp.
                            </p>
                        </div>

                    </div>
                </div>
            </section>
        </div>
    );
}
